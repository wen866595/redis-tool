package net.coderbee.redis.rdb

import net.coderbee.redis.RdbString

/**
 * rdb 文件解析的回调处理程序
 */
trait RdbVisitor {
    def onVersion(version: Int)

    def onDB(dbNum: Int)

    def onKV(key: RdbString, value: Any)

    def onKV(expireMills: Long, key: RdbString, value: Any)
    
    def onRdbEnd()
    
    def onError(err: Exception)
}