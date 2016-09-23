package hw1;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.FileWriter;
import java.util.Random;

public class CreateFile {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		try {
			FileWriter writer = new FileWriter("/home/hadoop/Desktop/workspace_hw1/Customers.txt",true);
			BufferedWriter bWriter = new BufferedWriter(writer);
			int i=0;
			for (i=0;i<50000;i++){
				bWriter.write(customers(i).toString()+"\n");
				
			}
			bWriter.flush();
			bWriter.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		try {
			FileWriter writer1 = new FileWriter("/home/hadoop/Desktop/workspace_hw1/Transactions.txt",true);
			BufferedWriter bWriter1 = new BufferedWriter(writer1);
			int i=0;
			for (i=0;i<5000000;i++){
				bWriter1.write(transaction(i).toString()+"\n");
			}
			bWriter1.flush();
			bWriter1.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		System.out.println("finish!");
	}
	
	public static StringBuffer customers(int k){
		StringBuffer cus = new StringBuffer();
		cus = cus.append(k+1).append(",").append(randomCharCus()).append(",").append(randInt(10,70)).append(",").append(randInt(1,10)).append(",").append(randFloat() + randInt(100,9999));
		return cus;
	}
	
	public static StringBuffer transaction(int k){
		StringBuffer transaction = new StringBuffer();
		transaction = transaction.append(k+1).append(",").append(randInt(1,49999)+1).append(",").append(randFloat() + randInt(10,999)).append(",").append(randInt(1,10)).append(",").append(randomCharTran());
		return transaction;
	}
	
	public static int randInt(int min, int max) {

	    Random rand1 = new Random();
	    int randomNum = rand1.nextInt((max - min) + 1) + min;
	    return randomNum;
	}
	
	public static float randFloat() {
		

	    Random rand2 = new Random();
	    float randomNum = rand2.nextFloat() ;
	    return randomNum;
	}
	
	public static String randomCharCus(){
		Random rand3 = new Random();
		String alphabet = "abcdefghigklmnopqrstuvwxyz";
		String ranChar = "";
		for (int i = 0; i < randInt(10,20); i++) {
	        
			char c = alphabet.charAt(rand3.nextInt(alphabet.length()));
			ranChar = ranChar + c;
	    }
		return ranChar;
	}
	
	public static String randomCharTran(){
		Random rand3 = new Random();
		String alphabet = "abcdefghigklmnopqrstuvwxyz";
		String ranChar = "";
		for (int i = 0; i < randInt(20,50); i++) {
	        
			char c = alphabet.charAt(rand3.nextInt(alphabet.length()));
			ranChar = ranChar + c;
	    }
		return ranChar;
	}
}
