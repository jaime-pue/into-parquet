/*
 * IntoParquet Copyright (c) 2025 Jaime Alvarez
 */

package com.github.jaime.intoParquet.service

import java.time.Duration
import java.time.Instant

class Chronometer {

    private val start: Instant   = now

    private def now: Instant = {
        Instant.now()
    }

    private def getDuration = {
        Duration.between(start, now)
    }

    override def toString: String = {
        f"${getDuration.getSeconds},${getDuration.getNano.toString.take(2)}"
//        f"${getDuration.toHours}%02d:${getDuration.toMinutes % 60}%02d:${getDuration.getSeconds % 60}%02d"
    }
}
