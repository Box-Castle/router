package com.box.castle.retry

class RetriesExceededException(msg: String, cause: Throwable) extends RuntimeException(msg, cause)
