package group_2;

import java.util.ArrayList;

public class Basic {
	
	public static void main(String[] arg) {
		//System.out.println(create_prime_numbers(10));
		System.out.println(hash(0, "Apple"));
	}

	public static long hash(int i, String s) {
		ArrayList<Integer> listPrime = create_prime_numbers(40);
		long res = ((listPrime.get(i)*s.hashCode()+listPrime.get(i+20)) % 1009 ) %1000;

		return res;
	}

	//produce required number of Prime numbers
	public static ArrayList<Integer> create_prime_numbers(int n) {
		int last_prime = 2;
		ArrayList<Integer> listPrime = new ArrayList<Integer>(n);
		listPrime.add(last_prime);
		int count = 1;
		boolean isenough = false;
		boolean isPrime = true;
		while(!isenough) {
			last_prime++;
			isPrime = true;
			for(int pr : listPrime) {
				if(last_prime%pr==0) {
					isPrime = false ; break;
				}
			}
			if(isPrime) {
				count++;
				listPrime.add(last_prime);
			}
			if(count>=n) isenough = true;
		}
		
		return listPrime;

	}
}
