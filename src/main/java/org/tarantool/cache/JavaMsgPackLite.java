package org.tarantool.cache;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

import org.tarantool.Code;
import org.tarantool.MsgPackLite;

/**
 * Extends MsgPack types to be packed and unpacked
 * <p>
 * Date types, BigDecimal and raw Java objects supported here
 * </p>
 *
 * @author Evgeniy Zaikin
 * @see MsgPackLite
 */
public class JavaMsgPackLite extends MsgPackLite {

    public static final JavaMsgPackLite INSTANCE = new JavaMsgPackLite();

    protected void packBin(Object item, OutputStream os) throws IOException {
        DataOutputStream out = new DataOutputStream(os);
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        try (ObjectOutputStream oos = new ObjectOutputStream(bos)) {
            oos.writeObject(item);
            oos.flush();
            out.write(MP_BIN32);
            out.writeInt(bos.size());
            bos.writeTo(out);
        } catch (IOException e) {
            out.write(MP_NULL);
            // eat this up
        }
    }

    @Override
    public void pack(Object item, OutputStream os) throws IOException {
        if (item == null) {
            super.pack(null, os);
        } else if (item instanceof Boolean) {
            super.pack(item, os);
        } else if (item instanceof Number || item instanceof Code) {
            super.pack(item, os);
        } else if (item instanceof String) {
            super.pack(item, os);
        } else if (item instanceof byte[] || item instanceof ByteBuffer) {
            super.pack(item, os);
        } else if (item instanceof List || item.getClass().isArray()) {
            super.pack(item, os);
        } else if (item instanceof Map) {
            super.pack(item, os);
        } else if (item instanceof Callable) {
            super.pack(item, os);
        } else {
            packBin(item, os);
        }
    }

    @Override
    protected Object unpackBin(int size, DataInputStream in) throws IOException {
        if (size < 0) {
            throw new IllegalArgumentException("byte[] to unpack too large for Java (more than 2^31 elements)!");
        }

        byte[] data = new byte[size];
        in.readFully(data);

        ByteArrayInputStream bos = new ByteArrayInputStream(data);
        try (ObjectInputStream ois = new ObjectInputStream(bos)) {
            return ois.readObject();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
            return null;
        }
    }
}
