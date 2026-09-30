package atomicflow

import atomicflow.Fingerprintable.{Fingerprint, Fingerprinter}

/** How a step reacts when one of its inputs differs from the recorded run. */
enum InputPolicy {
  /** Cached steps: [[InvalidateOn]]; at-most-once steps: [[EnsureUnchanged]]. */
  case Default

  /** A change re-runs the step (for at-most-once steps: deliberately performs the side effect again). */
  case InvalidateOn

  /** A change is a conflict ([[WorkflowError.StepConflict]]): the step never silently re-runs or reuses a result. */
  case EnsureUnchanged
}

case class StepInput[A: Fingerprintable](name: String, value: A, policy: InputPolicy = InputPolicy.Default) {
  def fingerprint(fingerprinter: Fingerprinter): Fingerprint = fingerprinter.fingerprint(value)

  def invalidateOn: StepInput[A] = copy(policy = InputPolicy.InvalidateOn)

  def ensureUnchanged: StepInput[A] = copy(policy = InputPolicy.EnsureUnchanged)
}

given [A: Fingerprintable] => Conversion[(String, A), StepInput[A]] = { (name: String, value: A) =>
  StepInput(name, value)
}

extension [A: Fingerprintable](input: (String, A)) {
  /** `("content" -> content).invalidateOn`: a change re-runs the step. */
  def invalidateOn: StepInput[A] = StepInput(input._1, input._2, InputPolicy.InvalidateOn)

  /** `("config" -> config).ensureUnchanged`: a change is a conflict. */
  def ensureUnchanged: StepInput[A] = StepInput(input._1, input._2, InputPolicy.EnsureUnchanged)
}
