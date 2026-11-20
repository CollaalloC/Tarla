package dev.tarla.client

import android.security.keystore.KeyGenParameterSpec
import android.security.keystore.KeyProperties
import android.util.AtomicFile
import io.flutter.embedding.android.FlutterActivity
import io.flutter.embedding.engine.FlutterEngine
import io.flutter.plugin.common.MethodChannel
import java.io.File
import java.io.FileNotFoundException
import java.security.KeyStore
import javax.crypto.Cipher
import javax.crypto.KeyGenerator
import javax.crypto.SecretKey
import javax.crypto.spec.GCMParameterSpec

class MainActivity : FlutterActivity() {
    private val keyAlias = "dev.tarla.paired-profiles.v1"
    private val blob get() = AtomicFile(File(noBackupFilesDir, "paired-profiles.gcm"))

    override fun configureFlutterEngine(flutterEngine: FlutterEngine) {
        super.configureFlutterEngine(flutterEngine)
        val messenger = flutterEngine.dartExecutor.binaryMessenger
        // A serial platform task queue keeps Keystore and fsync off the UI thread.
        MethodChannel(messenger, "dev.tarla/paired-profiles",
            io.flutter.plugin.common.StandardMethodCodec.INSTANCE,
            messenger.makeBackgroundTaskQueue()).setMethodCallHandler { call, result ->
            try {
                when (call.method) {
                    "read" -> result.success(readProfiles())
                    "write" -> {
                        writeProfiles(call.arguments as String)
                        result.success(null)
                    }
                    "clear" -> {
                        blob.delete()
                        check(!blob.baseFile.exists())
                        val store = keyStore()
                        if (store.containsAlias(keyAlias)) store.deleteEntry(keyAlias)
                        result.success(null)
                    }
                    else -> result.notImplemented()
                }
            } catch (_: Exception) {
                result.error("profile_storage", "Device profile storage is unavailable", null)
            }
        }
    }

    private fun keyStore() = KeyStore.getInstance("AndroidKeyStore").apply { load(null) }

    private fun key(create: Boolean): SecretKey {
        val stored = keyStore().getKey(keyAlias, null)
        if (stored is SecretKey) return stored
        check(create) { "Missing profile key" }
        val generator = KeyGenerator.getInstance(KeyProperties.KEY_ALGORITHM_AES, "AndroidKeyStore")
        generator.init(KeyGenParameterSpec.Builder(keyAlias,
            KeyProperties.PURPOSE_ENCRYPT or KeyProperties.PURPOSE_DECRYPT)
            .setBlockModes(KeyProperties.BLOCK_MODE_GCM)
            .setEncryptionPaddings(KeyProperties.ENCRYPTION_PADDING_NONE)
            .setKeySize(256)
            .build())
        return generator.generateKey()
    }

    private fun readProfiles(): String? {
        val file = blob
        val data = try {
            file.readFully()
        } catch (e: FileNotFoundException) {
            if (file.baseFile.exists()) throw e
            return null
        }
        require(data.size >= 1 + 12 + 16 && data[0] == 1.toByte())
        val cipher = Cipher.getInstance("AES/GCM/NoPadding")
        cipher.init(Cipher.DECRYPT_MODE, key(false), GCMParameterSpec(128, data, 1, 12))
        cipher.updateAAD(keyAlias.toByteArray(Charsets.UTF_8))
        return cipher.doFinal(data, 13, data.size - 13).toString(Charsets.UTF_8)
    }

    private fun writeProfiles(value: String) {
        val cipher = Cipher.getInstance("AES/GCM/NoPadding")
        cipher.init(Cipher.ENCRYPT_MODE, key(true))
        cipher.updateAAD(keyAlias.toByteArray(Charsets.UTF_8))
        val ciphertext = cipher.doFinal(value.toByteArray(Charsets.UTF_8))
        val iv = cipher.iv
        require(iv.size == 12)
        val file = blob
        val stream = file.startWrite()
        try {
            stream.write(1)
            stream.write(iv)
            stream.write(ciphertext)
            file.finishWrite(stream)
        } catch (e: Exception) {
            file.failWrite(stream)
            throw e
        }
    }
}
