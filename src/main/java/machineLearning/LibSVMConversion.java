package machineLearning;

import au.com.bytecode.opencsv.CSVReader;
import au.com.bytecode.opencsv.CSVWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.stream.IntStream;
import java.util.Map;
import java.util.HashMap;

public class LibSVMConversion {

    public LibSVMConversion() {
    }
    public static String processRecordTokens(String tokens[])
    {
        StringBuilder result = new StringBuilder();
        String temp = "";
        result.append("");

        for(int idx = 0; idx < tokens.length; idx++)
        {
            try {
                temp = tokens[idx];
            }
            catch(Exception ex){temp="";}
            if(temp != "")
                result.append(" ").append(idx+1).append(":").append(temp);
        }
        return result.toString();
    }

    public static String processRecord(String featureString, String classLabel, String separator)
    {
        String tokens[] = featureString.split(separator);
        StringBuilder result = new StringBuilder();
        double temp = 0;
        result.append("");
        if(classLabel != null)
            result.append(classLabel);

        for(int idx = 0; idx < tokens.length; idx++)
        {
            try {
                temp = Double.parseDouble(tokens[idx]);
            }
            catch(Exception ex){temp=0;}
            if(temp != 0)
                result.append(" ").append(idx+1).append(":").append(temp);
        }
        return result.toString();
    }

    public static Map<String, Integer> convertRecords(String _file_input, String _file_output, int class_pos, char _field_separator, int[] _disregard) throws IOException{
        CSVReader csv_reader = new CSVReader(new FileReader( _file_input), _field_separator);
        CSVWriter lsvm_writer = new CSVWriter(new FileWriter(_file_output),' ', CSVWriter.NO_QUOTE_CHARACTER, CSVWriter.NO_ESCAPE_CHARACTER, "\n");
        String classLabel="";
        String[] record_original;
        String[] record_libsvm = new String[1];
        Map<String, Integer> myMap = new HashMap<String, Integer>();
        int count = 0;
        while((record_original = csv_reader.readNext()) != null){
            String[] record_features = new String[record_original.length-1-_disregard.length];
            if (!record_original[0].contains("id")) {
                int record_index=0;
                classLabel = record_original[class_pos];
                for(int idx = 0; idx < record_original.length; idx++) {
                    int check = idx;
                    boolean found = IntStream.of(_disregard).anyMatch(n -> n == check);
                    if (idx != class_pos && !found){
                        if (idx == 0) {
                            boolean isKeyPresent = myMap.containsKey(record_original[idx]);
                            if (!isKeyPresent) {
                                myMap.put(record_original[idx], count);
                                count++;
                            }
                            record_features[record_index] = String.valueOf(myMap.get(record_original[idx]));
                            record_index++;
                        } else {
                            record_features[record_index] = record_original[idx];
                            record_index++;
                        }
                    }
                }

                record_libsvm[0] = classLabel.trim() + " " + processRecordTokens(record_features).trim();

                lsvm_writer.writeNext(record_libsvm);
            }
        }
        lsvm_writer.close();
        csv_reader.close();
        return myMap;
    }
}
