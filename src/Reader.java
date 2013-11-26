import java.io.File;
import java.io.IOException;

import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.FileReader;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;


public class Reader {

	public static void main(String[] args) throws IOException {
		// Deserialize Users from disk
		DatumReader<GenericRecord> datumReader = new GenericDatumReader<GenericRecord>();
		long count = 0;
		for(int i = 0; i < args.length; ++i) {
			
			File in = new File(args[i]);
			if(in.getName().startsWith("_")) continue;
			System.err.println("Opening file "+args[i]);
			FileReader<GenericRecord> dataFileReader = DataFileReader.openReader(in, datumReader);
			
			while (dataFileReader.hasNext()) {
				dataFileReader.next();
				count++;
			}
		}
		System.err.println("Got a count of "+count);
	}

}
