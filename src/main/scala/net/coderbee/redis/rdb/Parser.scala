package net.coderbee.redis.rdb

import java.io.InputStream
import java.util.Arrays
import java.util.zip.CRC32

import scala.collection.mutable

import org.h2.compress.CompressLZF

import net.coderbee.redis.RdbString

class Parser(ins: InputStream, visitor: RdbVisitor) {
    val MAGIC = "REDIS"
    val crc32 = new CRC32
    var rdbVersion: Int = 0

    def parse() {
        validMagic()

        rdbVersion = parseRdbVersion()
        visitor.onVersion(rdbVersion)

        var end = false
        do {
            val flag = readUint()

            flag match {
                // 指出 "有效期限时间是豪秒为单位". 在这之后，读取8字节无符号长整数作为有效期限时间
                case 0xFC =>
                    val mills = readLeLong(8)
                    val valType: Int = read()
                    decodeKV(mills, valType)

                // 指出 "有效期限时间是秒为单位". 在这之后，读取4字节无符号整数作为有效期限时间
                case 0xFD =>
                    val second = readLeLong(4)
                    val valType: Int = read()
                    decodeKV(second * 1000, valType)

                // 指出数据库选择器
                case 0xFE =>
                    val dbNum = parseDbNum()
                    visitor.onDB(dbNum)

                // RDB 文件结束指示器
                case 0xFF =>
                    end = true
                    visitor.onRdbEnd
                    if (rdbVersion >= 5) {
                        val excepted = read(4)
                        val get = crc32.getValue()
                    }

                // 这个键值对没有有效期限。$value_type 保证 != to FD, FC, FE and FF
                case _ => decodeKV(-1, flag)
            }
        } while (!end)
    }

    def decodeKV(expiredMills: Long, valueType: Int) {
        val key = readString()

        val value: Any =
            valueType match {
                case 0 => readString() // String 编码
                case 1 => decodeList() //  List 编码
                case 2 => decodeSet() //  Set 编码
                case 3 => decodeSortedset() //  Sorted Set 编码
                case 4 => decodeHash() //  Hash 编码
                case 9 => decodeZipmap() //  Zipmap 编码
                case 10 => decodeZiplist() //  Ziplist 编码
                case 11 => decodeIntset() //  IntSet 编码
                case 12 => decodeSortedsetInZiplist() //  以 Ziplist 编码的 Sorted Set
                case 13 => decodeHashmapInZiplist() //  以 Ziplist 编码的 Hashmap” （在rdb版本4中引入）
            }

        expiredMills match {
            case -1 => visitor.onKV(key, value)
            case _ => visitor.onKV(expiredMills, key, value)
        }
    }

    // Set[Tuple2[string, score]]
    def decodeSortedset(): Set[Tuple2[RdbString, RdbString]] = {
        val len = decodeLength()
        val set = new mutable.HashSet[Tuple2[RdbString, RdbString]]
        0.until(len).foreach(i => {
            val str = readString()
            val score = readString()
            set.+=((str, score))
        })
        set.toSet
    }

    def decodeZipmap(): Map[RdbString, RdbString] = {
        val rdbstr = readString()
        val buf = rdbstr.bytes
        val zmlen = buf(rdbstr.offset) & 0xFF

        val map = new mutable.HashMap[RdbString, RdbString]

        var index = rdbstr.offset + 1
        var end = false
        do {
            var klen = buf(index) & 0xFF

            if (klen != 0xFF) {
                val keyTuple = getZipmapLen(buf, klen, index + 1)

                klen = keyTuple._1
                index = keyTuple._2

                val key = new RdbString(buf, index, klen)
                index = index + klen

                var vlen = buf(index) & 0xFF
                val valueTuple = getZipmapLen(buf, vlen, index + 1)

                vlen = valueTuple._1
                index = valueTuple._2

                val free = buf(index) & 0xFF
                index = index + 1

                val value = new RdbString(buf, index, vlen)
                index = index + vlen

                // 跳过空闲字节
                index = index + free

                map.+=((key, value))

            } else {
                end = true
            }
        } while (!end)

        map.toMap
    }

    def getZipmapLen(buf: Array[Byte], len: Int, start: Int): Tuple2[Int, Int] = {
        len match {
            case 254 | 255 => throw new IllegalStateException("could not be 254 or 255 !")
            case 253 => (getLeInt(buf, start, 4), start + 4)
            case _ => (len, start) // < 253
        }
    }

    def decodeSortedsetInZiplist(): Set[RdbString] = decodeZiplist().toSet

    def decodeHashmapInZiplist(): Map[RdbString, RdbString] = {
        val list = decodeZiplist()
        val map = new mutable.HashMap[RdbString, RdbString]
        var i = 0
        while (i < list.length) {
            map.put(list.apply(i), list.apply(i + 1))
            i = i + 2
        }
        map.toMap
    }

    def decodeIntset(): Set[RdbString] = {
        val rdbstr = readString()
        val buf = rdbstr.bytes
        val byteNum = getLeInt(buf, rdbstr.offset + 0, 4) // 2,4,8
        val contentNum = getLeInt(buf, rdbstr.offset + 4, 4)

        val set = new mutable.HashSet[RdbString]
        val start = rdbstr.offset + 8
        0.until(contentNum).foreach(i => set.add(new RdbString(getLittleEndianLong(buf, start + i * byteNum, byteNum))))

        set.toSet
    }

    def decodeZiplist(): List[RdbString] = {
        val rdbstr = readString()
        val buf = rdbstr.bytes

        val zlbytes = getLeInt(buf, rdbstr.offset + 0, 4)
        val zltail = getLeInt(buf, rdbstr.offset + 4, 4)
        val zllen = getLeInt(buf, rdbstr.offset + 8, 2)

        val list = new mutable.MutableList[RdbString]
        var start = rdbstr.offset + 10
        0.until(zllen).foreach(i => {
            val t2 = decodeZiplistEntry(buf, start)
            start = t2._2
            list.+=(t2._1)
        })

        list.toList
    }

    def preEntryLength(buf: Array[Byte], start: Int): Tuple2[Int, Int] = {
        val eLen = buf(start) & 0xFF
        eLen match {
            case 255 => throw new IllegalStateException("should not be 255 .")
            case 254 =>
                val len = getLeInt(buf, start + 1, 4)
                (len, start + 5)

            case _ => (eLen, start + 1)
        }
    }

    // 返回： (条目类型c:字符串 i:整数,  rawBytes长度,  新下标位置)
    def decodeSpecialFlag(buf: Array[Byte], start: Int): Tuple3[Char, Int, Int] = {
        var index = start
        var rawBytesLen: Long = 0
        var flag = 'c' // 默认假设条目是字符串的
        val specialFlag = buf(index) & 0xFF
        index = index + 1

        specialFlag >>> 6 match {
            case 0x00 => rawBytesLen = specialFlag

            case 0x01 => {
                rawBytesLen = ((specialFlag & 0x3F) << 8) | buf(index) & 0xFF
                index = index + 1
            }
            case 0x02 => {
                rawBytesLen = getBigEndianLong(buf, index, 4)
                index = index + 4
            }

            case _ => { // 有符号整数
                flag = 'i' // 条目是整数

                specialFlag >>> 4 match {
                    case 0x0C => {
                        rawBytesLen = getBigEndianLong(buf, index, 2)
                        index = index + 2
                    }
                    case 0x0D => {
                        rawBytesLen = getBigEndianLong(buf, index, 4)
                        index = index + 4
                    }
                    case 0x0E => {
                        rawBytesLen = getBigEndianLong(buf, index, 8)
                        index = index + 8
                    }
                    case 0x0F => {
                        specialFlag match {
                            case 0xF0 =>
                                rawBytesLen = getBigEndianLong(buf, index, 3)
                                index = index + 3

                            case 0xFE =>
                                rawBytesLen = getBigEndianLong(buf, index, 1)
                                index = index + 1

                            case _ =>
                                rawBytesLen = (specialFlag & 0x0F) // - 1
                        }
                    }
                }
            }
        }

        val rawBytesLenth = rawBytesLen.asInstanceOf[Int]

        (flag, rawBytesLenth, index)
    }

    def decodeRawBytesOfEntry(buf: Array[Byte], index: Int, rawBytesLenth: Int): Tuple2[Int, Array[Byte]] = {
        val rawBytes: Array[Byte] = Arrays.copyOfRange(buf, index, index + rawBytesLenth)
        (index + rawBytesLenth, rawBytes)
    }

    def decodeZiplistEntry(buf: Array[Byte], start: Int): Tuple2[RdbString, Int] = {
        val preEntryTuple = preEntryLength(buf, start)

        val flagTuple = decodeSpecialFlag(buf, preEntryTuple._2)

        if (flagTuple._1 == 'c') {
            val bytesTuple = decodeRawBytesOfEntry(buf, flagTuple._3, flagTuple._2)
            val str = new RdbString(bytesTuple._2)
            (str, bytesTuple._1)
        } else {
            val long = new RdbString(flagTuple._2)
            (long, flagTuple._3)
        }
    }

    def decodeHash(): mutable.Map[RdbString, RdbString] = {
        val size = decodeLength()
        val map = new mutable.ListMap[RdbString, RdbString]()

        0.until(size).foreach(i => {
            val key = readString()
            val value = readString()
            map.put(key, value)
        })
        map
    }

    def decodeSet(): Set[RdbString] = decodeList().toSet

    def decodeList(): Seq[RdbString] = {
        val size = decodeLength()
        val list = new mutable.ArrayBuffer[RdbString](size)
        0.until(size).foreach(i => {
            val e = readString()
            list.append(e)
        })
        list
    }

    def validMagic() {
        val magic = read(MAGIC.length())
        val magicStr = new String(magic)
        if (!MAGIC.equals(magicStr)) {
            throw new IllegalStateException("not a valid rdb file .")
        }
    }

    def parseRdbVersion(): Int = {
        val num = read(4)
        val numStr = new String(num)
        Integer.parseInt(numStr)
    }

    def parseDbNum(): Int = {
        val b = read()
        decodeNormalLength(b)
    }

    def readString(): RdbString = {
        val b = read() & 0xFF
        b >>> 6 match {
            case 0x03 => {
                b & 0x3F match {
                    // 0 表示接下来是8bit整数
                    case 0 =>
                        val i8 = read()
                        new RdbString(i8.byteValue)

                    // 1 表示接下来是16bit整数
                    case 1 =>
                        val i16 = readUint() | read() << 8
                        new RdbString(i16.shortValue)

                    // 2 表示接下来是32bit整数
                    case 2 =>
                        val i32 = readUint() | readUint() << 8 | readUint() << 16 | read() << 24
                        new RdbString(i32)

                    // 4 表示 LZF 压缩字符串
                    case 3 =>
                        val clen = decodeLength() // 压缩后的长度
                        val uclen = decodeLength() // 未压缩长度
                        val buf = read(clen)

                        val original = new Array[Byte](uclen)
                        val compressor = new CompressLZF()
                        compressor.expand(buf, 0, clen, original, 0, uclen)
                        new RdbString(original)
                }
            }
            case _ => { // 前缀长度编码的字符串
                val len = decodeNormalLength(b)
                val buf = new Array[Byte](len)
                val readed = ins.read(buf)
                new RdbString(buf)
            }
        }
    }

    // 整数是高位优先
    def decodeNormalLength(b: Int): Int = {
        b >>> 6 match {
            case 0 => b
            case 1 => (b & 0x3F) << 8 | read() & 0xFF
            case 2 =>
                val buf = read(4)
                getBigEndianLong(buf, 0, 4).asInstanceOf[Int]
        }
    }

    // 解析 长度 编码
    def decodeLength(): Int = {
        val b = readUint()

        b >>> 6 match {
            case 0 | 1 | 2 => decodeNormalLength(b)
            case 0x03 => getInt(b & 0x3F)
        }
    }

    def getInt(b: Int): Int = {
        b match {
            case 0 => readUint()
            case 1 => (readUint() << 8) | readUint()
            case 2 =>
                val buf = read(4)
                getBigEndianLong(buf, 0, 4).asInstanceOf[Int]
        }
    }

    def readLeLong(byteCount: Int): Long = {
        val bytes = read(byteCount)
        getLittleEndianLong(bytes, 0, byteCount)
    }

    // 按 little endian 编码获取整数
    def getLeInt(buf: Array[Byte], start: Int, len: Int): Int = getLittleEndianLong(buf, start, len).asInstanceOf[Int]

    def getBigEndianLong(buf: Array[Byte], start: Int, len: Int): Long = {
        var long: Long = buf(start) & 0xFF
        (start + 1).until(start + len).foreach(i => long = long << 8 | ((buf(i) & 0xFF)).asInstanceOf[Long])
        long
    }

    def getLittleEndianLong(buf: Array[Byte], start: Int, len: Int): Long = {
        var long: Long = buf(start) & 0xFF
        1.until(len).foreach(i => {
            val b = buf(start + i) & 0xFF
            long = long | (b.asInstanceOf[Long] << 8 * i)
        })
        long
    }

    def readUint(): Int = read() & 0xFF

    def read(): Int = {
        val b = ins.read()

        if (b == -1) {
            throw new IllegalStateException("should not be end of file .")
        }

        if (rdbVersion >= 5) {
            crc32.update(b)
        }
        b
    }

    def read(count: Int): Array[Byte] = {
        val buff = new Array[Byte](count)
        var n = 0
        var readed = 0
        while (n < count) {
            readed = ins.read(buff, n, count - n)
            if (readed == -1) {
                throw new IllegalStateException("should not be end of file .")
            }
            n = n + readed
        }

        if (rdbVersion >= 5) {
            crc32.update(buff)
        }
        buff
    }
}
