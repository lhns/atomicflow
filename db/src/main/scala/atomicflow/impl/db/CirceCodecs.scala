package atomicflow.impl.db

import atomicflow.Fingerprintable.Fingerprint
import atomicflow.internal.StepInputFingerprints
import cats.syntax.all.*
import io.circe.Codec

object CirceCodecs {
  given Codec[Fingerprint] = Codec.implied[String].imap(Fingerprint.fromString)(_.toString)

  given Codec[StepInputFingerprints] =
    Codec.implied[Map[String, Fingerprint]].imap(StepInputFingerprints(_))(_.fingerprints)
}
