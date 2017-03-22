package com.box.castle.router

class RouterException(val msg: String, e: Throwable = null) extends RuntimeException(msg, e)
