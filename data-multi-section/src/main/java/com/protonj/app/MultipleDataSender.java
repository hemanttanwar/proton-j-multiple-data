package com.protonj.app;

import org.apache.qpid.proton.Proton;
import org.apache.qpid.proton.amqp.Binary;
import org.apache.qpid.proton.amqp.messaging.Data;
import org.apache.qpid.proton.codec.impl.DataImpl;
import org.apache.qpid.proton.engine.Sender;
import org.apache.qpid.proton.message.Message;

import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * Trying to understand how we can send multiple data section in one amqp message using proton-j library.
 *
 * Following suggestion from : https://issues.apache.org/jira/browse/PROTON-1098
 */
public class MultipleDataSender {
    final int MAX_SIZE_BYTES = 1024;
    final int HEADER_SIZE_BYTES = 10;

    Sender sender; // In real application this will be initialised.

    public void send( ) {
        // Lets assume we want to add two data sections.
        final int totalAdditionalSectionToSend = 2;

        final byte[] bytes = new byte[MAX_SIZE_BYTES];
        final int payloadSize = 100; //  assumed
        final int messageAllocationSize = Math.min(payloadSize + HEADER_SIZE_BYTES, MAX_SIZE_BYTES);

        // 1. create first Message

        final Message message = Proton.message();
        final byte[] body = "First section. ".getBytes();
        Binary binary = new Binary(body);
        message.setBody(new Data(binary));


        int encodedSize = message.encode(bytes, 0, messageAllocationSize);

        int byteArrayOffset = encodedSize;

        //2.  Now append above created bytes with additional Section to send in this amqp message.
        for (int i = 0; i < totalAdditionalSectionToSend; ++i) {

            // create codec Data with just bytes as body.
            org.apache.qpid.proton.codec.Data messageWrappedByData = DataImpl.Factory.create();

            messageWrappedByData.putDescribedType(new AmqpDataDescribedType(new Binary((" additional section -" + (i + 2))
                    .getBytes(UTF_8))));

            final byte[] bytesWrappedData = messageWrappedByData.encode().getArray();

            int additionalSectionEncodedSize = bytesWrappedData.length;

            // append at end of bytes
            int index = byteArrayOffset;
            for (int j = 0; j < additionalSectionEncodedSize && index < MAX_SIZE_BYTES; ++j) {
                bytes[index++] = bytesWrappedData[j];
            }

            byteArrayOffset = byteArrayOffset + additionalSectionEncodedSize;
        } //for

        // send one amqp message with multiple data sections.
        sender.send(bytes, 0, byteArrayOffset);

    }
}