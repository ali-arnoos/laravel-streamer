<?php

declare(strict_types=1);

namespace Tests;

use Illuminate\Foundation\Testing\Concerns\InteractsWithRedis;
use Illuminate\Redis\Connections\Connection;
use Illuminate\Support\Facades\Redis;
use Prwnr\Streamer\Archiver\StreamArchiver;
use Prwnr\Streamer\Errors\MessagesRepository;
use Prwnr\Streamer\EventDispatcher\Message;
use Prwnr\Streamer\EventDispatcher\ReceivedMessage;
use Prwnr\Streamer\EventDispatcher\Streamer;
use Prwnr\Streamer\Exceptions\MixedStreamConnectionsException;
use Prwnr\Streamer\Facades\Streamer as StreamerFacade;
use Prwnr\Streamer\History\EventHistory;
use Prwnr\Streamer\Stream;
use Prwnr\Streamer\Stream\MultiStream;
use Prwnr\Streamer\Streams;
use Tests\Stubs\LocalListener;
use Tests\Stubs\OtherBarStreamerEventStub;

/**
 * foo.bar is mapped to the "cache" connection (database 6); everything else stays on
 * "default" (database 5) of the same server, so each assertion can tell them apart.
 */
class StreamConnectionsTest extends TestCase
{
    use InteractsWithRedis;
    use WithMemoryManager;

    protected function setUp(): void
    {
        parent::setUp();
        $this->setUpRedis();
        $this->redis['phpredis']->connection()->flushall();

        // setUpRedis() provides "default" (database 5) and "cache" (database 6).
        $this->app['config']->set('streamer.connections', ['foo.bar' => 'cache']);
        $this->app['config']->set('streamer.listen_timeout', 0.01);
        $this->app['config']->set('streamer.stream_read_timeout', 0.01);

        $this->setUpMemoryManager();
    }

    protected function tearDown(): void
    {
        $this->redis['phpredis']->connection()->flushall();
        $this->tearDownRedis();
        parent::tearDown();

        $this->manager = null;
    }

    public function test_resolves_mapped_streams_and_falls_back_for_the_rest(): void
    {
        $this->assertSame('cache', Stream::connectionFor('foo.bar'));
        $this->assertSame('default', Stream::connectionFor('other.bar'));

        $this->app['config']->set('streamer.redis_connection', 'custom');
        $this->assertSame('cache', Stream::connectionFor('foo.bar'));
        $this->assertSame('custom', Stream::connectionFor('other.bar'));

        $this->assertSame('cache', (new Stream('foo.bar'))->getConnectionName());
    }

    public function test_emits_each_event_on_its_streams_connection(): void
    {
        StreamerFacade::emit($this->makeEvent());
        StreamerFacade::emit(new OtherBarStreamerEventStub());

        $this->assertSame(1, $this->side()->xLen('foo.bar'));
        $this->assertSame(0, $this->main()->xLen('foo.bar'));
        $this->assertSame(1, $this->main()->xLen('other.bar'));
        $this->assertSame(0, $this->side()->xLen('other.bar'));
    }

    public function test_listens_as_consumer_group_on_a_mapped_stream(): void
    {
        // Long enough for the first read; cancel() ends the loop as soon as it arrives.
        $this->app['config']->set('streamer.listen_timeout', 2);
        (new Stream('foo.bar'))->createGroup('bar');
        $streamer = new Streamer(new EventHistory());
        $id = $streamer->emit($this->makeEvent());

        $received = [];
        $streamer->asConsumer('foobar', 'bar');
        $streamer->startFrom('0-0');
        $streamer->listen('foo.bar', function (ReceivedMessage $message, Streamer $streamer) use (&$received): void {
            $received[] = $message->getId();
            $streamer->cancel();
        });

        $this->assertSame([$id], $received);
        // Acknowledged on the stream's own connection.
        $this->assertSame(0, $this->side()->xPending('foo.bar', 'bar')[0]);
    }

    public function test_listen_command_purges_from_the_mapped_stream(): void
    {
        $this->withLocalListenersConfigured([LocalListener::class]);
        $id = StreamerFacade::emit($this->makeEvent());

        $this->artisan('streamer:listen', ['events' => 'foo.bar', '--last_id' => '0-0', '--purge' => true])
            ->expectsOutput("Message [$id] has been purged from the 'foo.bar' stream.")
            ->assertExitCode(0);

        $this->assertSame(0, $this->side()->xLen('foo.bar'));
    }

    public function test_archives_from_and_restores_to_the_mapped_stream(): void
    {
        $id = StreamerFacade::emit($this->makeEvent());
        $received = new ReceivedMessage($id, $this->side()->xRange('foo.bar', '-', '+')[$id]);

        /** @var StreamArchiver $archiver */
        $archiver = $this->app->make(StreamArchiver::class);
        $archiver->archive($received);

        $this->assertSame(0, $this->side()->xLen('foo.bar'));
        $archived = $this->manager->driver('memory')->find('foo.bar', $id);
        $this->assertInstanceOf(Message::class, $archived);

        $archiver->restore($archived);

        $this->assertSame(1, $this->side()->xLen('foo.bar'));
        $this->assertSame(0, $this->main()->xLen('foo.bar'));
    }

    public function test_retries_a_failed_message_from_the_mapped_stream(): void
    {
        $this->failFakeMessage('foo.bar', '123-0', ['payload' => 123]);

        // The failed-messages set stays on the default connection; the message on the stream's.
        $this->assertSame(1, $this->main()->sCard(MessagesRepository::ERRORS_SET));
        $this->assertSame(1, $this->side()->xLen('foo.bar'));

        $this->expectsListenersToBeCalled([LocalListener::class]);
        $this->artisan('streamer:failed:retry', ['--all' => true])
            ->expectsOutput(sprintf('Successfully retried [123-0] on foo.bar stream by [%s] listener', LocalListener::class))
            ->assertExitCode(0);

        $this->assertSame(0, $this->main()->sCard(MessagesRepository::ERRORS_SET));
    }

    public function test_multi_stream_rejects_streams_on_different_connections(): void
    {
        $this->expectException(MixedStreamConnectionsException::class);
        $this->expectExceptionMessage('foo.bar (cache), other.bar (default)');

        new MultiStream(['foo.bar', 'other.bar']);
    }

    public function test_listen_command_rejects_streams_on_different_connections(): void
    {
        $this->withLocalListenersConfigured([LocalListener::class]);

        $this->expectException(MixedStreamConnectionsException::class);

        $this->artisan('streamer:listen', ['events' => 'foo.bar,other.bar', '--group' => 'bar']);
    }

    public function test_multi_stream_reads_streams_sharing_a_mapped_connection(): void
    {
        $this->app['config']->set('streamer.connections', ['foo.bar' => 'cache', 'other.bar' => 'cache']);
        StreamerFacade::emit($this->makeEvent());
        StreamerFacade::emit(new OtherBarStreamerEventStub());

        $messages = (new MultiStream(['foo.bar', 'other.bar']))->await('0-0', 0.01);

        $this->assertCount(2, $messages);
        $this->assertSame(0, $this->main()->exists('foo.bar', 'other.bar'));
    }

    public function test_streams_adds_to_each_streams_connection_and_reads_only_within_one(): void
    {
        $streams = new Streams(['foo.bar', 'other.bar']);
        $streams->add($this->makeMessage());

        $this->assertSame(1, $this->side()->xLen('foo.bar'));
        $this->assertSame(1, $this->main()->xLen('other.bar'));

        $this->expectException(MixedStreamConnectionsException::class);
        $streams->read();
    }

    private function main(): Connection
    {
        return $this->unprefixed('default');
    }

    private function side(): Connection
    {
        return $this->unprefixed('cache');
    }

    private function unprefixed(string $name): Connection
    {
        $connection = Redis::connection($name);
        $connection->setOption(\Redis::OPT_PREFIX, '');

        return $connection;
    }
}
