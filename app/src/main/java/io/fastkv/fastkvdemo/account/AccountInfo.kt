package io.fastkv.fastkvdemo.account

import io.fastkv.fastkvdemo.fastkv.kvdelegate.NullableObjectEncoder
import io.fastkv.interfaces.FastEncoder
import io.packable.PackCreator
import io.packable.PackDecoder
import io.packable.PackEncoder
import io.packable.Packable

data class AccountInfo(
    var uid: Long,
    var token: String,
    var nickname: String,
    var phoneNo: String,
    var email: String
) : Packable {
    override fun encode(encoder: PackEncoder) {
        encoder
            .putLong(0, uid)
            .putString(1, token)
            .putString(2, nickname)
            .putString(3, phoneNo)
            .putString(4, email)
    }

    companion object {
        val CREATOR: PackCreator<AccountInfo> = PackCreator {
            AccountInfo(
                it.getLong(0),
                it.getString(1),
                it.getString(2),
                it.getString(3),
                it.getString(4),
            )
        }

        val ENCODER = object : NullableObjectEncoder<AccountInfo> {
            override fun encode(obj: AccountInfo?): ByteArray? {
                if(obj == null) return null
                return PackEncoder.marshal(obj)
            }

            override fun decode(data: ByteArray?): AccountInfo? {
                if(data == null) return null
                return PackDecoder.unmarshal(data, CREATOR)
            }
        }
    }
}
