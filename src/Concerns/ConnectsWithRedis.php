<?php

declare(strict_types=1);

namespace Prwnr\Streamer\Concerns;

use Illuminate\Redis\Connections\Connection;
use Illuminate\Support\Facades\Redis;
use Prwnr\Streamer\Exceptions\MixedStreamConnectionsException;

trait ConnectsWithRedis
{
    /**
     * Redis connection for a stream: its entry in streamer.connections, or
     * streamer.redis_connection when it has none.
     */
    public static function connectionFor(string $stream): string
    {
        // Indexed directly: stream names may contain dots, which config() would split.
        $connections = config('streamer.connections') ?? [];

        return $connections[$stream]
            ?? config('streamer.redis_connection')
            ?? 'default';
    }

    /**
     * The one Redis connection shared by streams read in a single command.
     *
     * @param array<string> $streams
     * @throws MixedStreamConnectionsException when they resolve to different connections
     */
    public static function sharedConnectionFor(array $streams): string
    {
        $connections = [];
        foreach ($streams as $stream) {
            $connections[$stream] = static::connectionFor($stream);
        }

        if (count(array_unique($connections)) > 1) {
            throw MixedStreamConnectionsException::forStreams($connections);
        }

        return reset($connections) ?: static::connectionFor('');
    }

    /**
     * Returns Redis connection based on configuration.
     */
    protected function redis(): Connection
    {
        $connection = Redis::connection($this->redisConnectionName());
        $connection->setOption(\Redis::OPT_PREFIX, '');

        return $connection;
    }

    /**
     * Name of the Redis connection to use. Classes bound to a stream override
     * this to use the stream's own connection.
     */
    protected function redisConnectionName(): string
    {
        return config('streamer.redis_connection') ?? 'default';
    }
}
