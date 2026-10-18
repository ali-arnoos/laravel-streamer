<?php

declare(strict_types=1);

namespace Prwnr\Streamer;

use Prwnr\Streamer\Concerns\ConnectsWithRedis;
use Prwnr\Streamer\Contracts\Errors\StreamableMessage;

class Streams
{
    use ConnectsWithRedis;

    private ?string $connection = null;

    public function __construct(private readonly array $streams)
    {
    }

    public function add(StreamableMessage $message, string $id = '*'): array
    {
        $ids = [];
        foreach ($this->streams as $stream) {
            // Each stream on its own connection.
            $ids[] = (new Stream($stream))->add($message, $id);
        }

        return $ids;
    }

    /**
     * @throws \Prwnr\Streamer\Exceptions\MixedStreamConnectionsException when the streams
     *         are on different Redis connections: they are read in one command
     */
    public function read(array $from = [], ?int $limit = null): array
    {
        $this->connection = static::sharedConnectionFor(array_values($this->streams));

        $read = [];
        foreach ($this->streams as $key => $stream) {
            $read[$stream] = $from[$key] ?? Stream::FROM_START;
        }

        $result = [];
        if ($limit) {
            $result = $this->redis()->xRead($read, $limit);
        }

        if (!$limit) {
            $result = $this->redis()->xRead($read);
        }

        if (!is_array($result)) {
            return [];
        }

        return $result;
    }

    protected function redisConnectionName(): string
    {
        return $this->connection ?? config('streamer.redis_connection') ?? 'default';
    }
}
