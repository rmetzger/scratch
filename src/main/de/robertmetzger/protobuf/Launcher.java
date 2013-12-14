package de.robertmetzger.protobuf;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;

import org.apache.commons.lang.RandomStringUtils;

import com.google.protobuf.CodedOutputStream;

import de.robertmetzger.protobuf.generated.Map;
import de.robertmetzger.protobuf.generated.Map.KeyValue;
import de.robertmetzger.protobuf.generated.Map.KeyValue.Builder;

public class Launcher {
	private static KeyValue generateKeyValue() {
		Builder person = Map.KeyValue.newBuilder();

		person.setKey("k-" + RandomStringUtils.randomAlphabetic(15));
		person.setValue("v-" + RandomStringUtils.randomAlphabetic(35));
		return person.build();
	}

	public static void main(String[] args) {

		try {
			String[] env =  {"PYTHONPATH=src/python"};
			Process p = Runtime.getRuntime().exec("python2 test.py", env);
			
			BufferedReader input = new BufferedReader(new InputStreamReader(p.getInputStream()));
			BufferedReader err = new BufferedReader(new InputStreamReader(p.getErrorStream()));
			OutputStream processInput = p.getOutputStream();
			for (int i = 0; i < 2; i++) {
				KeyValue kv = generateKeyValue();
				System.err.println("ser size "+kv.getSerializedSize());
				System.err.println("sending key "+kv.getKey() );
				kv.writeDelimitedTo(processInput);
			}
			// write -1 
			final CodedOutputStream codedOutput = CodedOutputStream.newInstance(processInput, 1);
			codedOutput.writeRawVarint32(-1);
			codedOutput.flush();
			processInput.close();
			
			String line;
			while ((line = input.readLine()) != null) {
				System.err.println("Python: '"+line);
			}
			while ((line = err.readLine()) != null) {
				System.err.println("Python Error: "+line);
			}
		} catch (IOException e) {
			e.printStackTrace();
		}

		// kv.writeDelimitedTo(output);
	}
}
