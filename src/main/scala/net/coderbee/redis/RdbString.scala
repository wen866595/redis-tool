package net.coderbee.redis

case class RdbString(val vtype: Byte, val bytes: Array[Byte], val offset: Int, val length: Int, val long: Long) {
    val CHARSET = "UTF-8"

    def this(bytes: Array[Byte], offset: Int, length: Int) = this(RdbString.STRING, bytes, offset, length, 0)

    def this(bytes: Array[Byte]) = this(RdbString.STRING, bytes, 0, bytes.length, 0)

    def this(long: Long) = this(RdbString.LONG, null, 0, 0, long)

    override def toString(): String = {
        if (vtype == RdbString.STRING)
            new String(bytes, offset, length, CHARSET)
        else
            java.lang.Long.toString(long)
    }
}

object RdbString {
    val STRING: Byte = 0
    val LONG: Byte = 1
}