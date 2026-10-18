<?php

declare(strict_types=1);

namespace Prwnr\Streamer\Exceptions;

use InvalidArgumentException;

class MixedStreamConnectionsException extends InvalidArgumentException
{
    /**
     * @param array<string, string> $connections stream => connection
     */
    public static function forStreams(array $connections): self
    {
        $pairs = [];
        foreach ($connections as $stream => $connection) {
            $pairs[] = "$stream ($connection)";
        }

        return new self(
            'Streams read together must share one Redis connection, got: ' . implode(', ', $pairs)
            . '. Run one listener per connection.'
        );
    }
}
