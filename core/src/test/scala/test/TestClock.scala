package test

import java.time.{Clock, Instant, ZoneId, ZoneOffset}
import java.util.concurrent.atomic.AtomicReference
import scala.concurrent.duration.FiniteDuration

/** A clock that only moves when a test advances it, so time-based behavior (lock expiry, retry backoff) is tested
  * deterministically instead of by waiting. */
final class TestClock(start: Instant = Instant.now()) extends Clock {
  private val current = new AtomicReference[Instant](start)

  def advance(duration: FiniteDuration): Unit =
    current.updateAndGet(_.plusNanos(duration.toNanos))

  override def instant(): Instant = current.get()

  override def getZone: ZoneId = ZoneOffset.UTC

  override def withZone(zone: ZoneId): Clock = this
}
