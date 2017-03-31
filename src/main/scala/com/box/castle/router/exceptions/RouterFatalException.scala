package com.box.castle.router.exceptions

/**
 * Exceptions of this type indicate that the process will be in an unknown state and cannot be recovered safely
 */
private[castle] class RouterFatalException(msg: String, t: Throwable = null) extends RuntimeException(msg, t)
