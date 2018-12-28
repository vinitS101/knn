//Compares classes of Output File and Test File, and produces the accuracy(%)

import java.io.*;
import java.util.*;
class testAccuracy
{
    public static void main(String[] args) throws IOException {

        FileInputStream ob1 = new FileInputStream("/SamepleData/KnnTestingData.txt");
        FileInputStream ob2 = new FileInputStream("/SamepleData/SampleOutput.txt");

        BufferedReader br1 = new BufferedReader(new InputStreamReader(ob1));
        BufferedReader br2 = new BufferedReader(new InputStreamReader(ob2));
        
        String str1 = br1.readLine();
        String str2 = br2.readLine();
        double den = 0, num = 0;

        while(str1 != null && str1.length() != 0 && str2 != null && str2.length() != 0) {
            
            char ch2[] = str2.toCharArray();
            char ch1[] = str1.toCharArray();

            if( ch1[str1.length()-1] != ch2[0] ) {
                den++;
            } 
            else {
                den++;
                num++;
            }

            str1 = br1.readLine();
            str2 = br2.readLine();
        }

        System.out.println(num+" "+den);
        System.out.println(num/den);
    }
}