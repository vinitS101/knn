import java.io.*;
import java.util.*;
class test5
{
    public static void main(String[] args) throws IOException {
        FileInputStream ob1=new FileInputStream("F:/text.txt");
        FileInputStream ob2=new FileInputStream("F:/output.txt");
        BufferedReader br1=new BufferedReader(new InputStreamReader(ob1));
        BufferedReader br2=new BufferedReader(new InputStreamReader(ob2));
        String str1= br1.readLine();
        String str2=br2.readLine();
        double den=0,num=0;
        while(str1!=null && str1.length()!=0 && str2!=null && str2.length()!=0)
        {
            char ch2[]=str2.toCharArray();
            char ch1[]= str1.toCharArray();
            if(ch1[str1.length()-1]!=ch2[0])
                den++;
            else
            {
                den++;
                num++;
            }
            str1=br1.readLine();
            if(str1!=null && str1.length()==0)
                str1 = br2.readLine();
            str2=br2.readLine();
            if(str2!= null && str2.length()==0)
                str2 = br2.readLine();

        }

        System.out.println(num+" "+den);
        double accuracy=num/den;
        System.out.println(accuracy);



    }
}