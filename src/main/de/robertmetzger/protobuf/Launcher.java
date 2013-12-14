package de.robertmetzger.protobuf;

import java.io.IOException;
import java.io.OutputStream;

import org.apache.commons.lang.RandomStringUtils;

import de.robertmetzger.protobuf.generated.Map;
import de.robertmetzger.protobuf.generated.Map.KeyValue;
import de.robertmetzger.protobuf.generated.Map.KeyValue.Builder;

public class Launcher {
	private static KeyValue generateKeyValue() {
		Builder person = Map.KeyValue.newBuilder();
		
		person.setKey("k-"+RandomStringUtils.randomAlphabetic(15));
		person.setValue("v-"+RandomStringUtils.randomAlphabetic(35));
		return person.build();
	}
	public static void main(String[] args) {
		
		try {
			Process p = Runtime.getRuntime().exec("python test.py");
			OutputStream processInput = p.getOutputStream();
			for(int i = 0; i < 15; i++)
				generateKeyValue().writeTo(processInput);
			processInput.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
	//	kv.writeDelimitedTo(output);
	}
}
