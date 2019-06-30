//@CopyRight -> Parsa Badiei
//group #2 - Parsa Badiei - MA Masoud - Christian Zaharia
package group_2;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Set;

public class UserSet implements Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = 6802073102855555005L;
	//constructor for the class using the artist name 
	public UserSet(String artist, Set<String> userset) {
		this.Artist = artist;
		this.userSet = userset;
	}
	public UserSet() {
		
	}

	
public Set<String> userSet = new HashSet<String>();
public String Artist;
public String minHashSignature ;

//add a single user to the user set (overloaded method)
public Set<String> add (String username) {
	this.userSet.add(username);
	return this.userSet;
}
//add another set to the user set (overloaded method)
public Set<String> add (Set<String> userset){
	this.userSet.addAll(userset);
	return this.userSet;
}
//calculate Jacardian distance 
public double distanceTo(Set<String> other) {
	HashSet<String> Union = new HashSet<String>(this.userSet);
	Union.addAll(other);
	double union = Union.size();
	
	HashSet<String> Common = new HashSet<String>(this.userSet);
	Common.retainAll(other);
	double common = Common.size();
	//the returned value might need some precision control (might return too many digits)
	return common/union;
}

//calculate all 20 permutations of the min hash value -> Minimum Hash Signature
public String toMinHashSignature() {
	String usaerSetString = this.userSet.toString();
	String minHashSignature = Basic.hash(0, usaerSetString)+"";
	for(int i =1 ; i<20 ; i++) {
		minHashSignature+="," + Basic.hash(i, usaerSetString);
	}
	this.minHashSignature = minHashSignature;
	return minHashSignature;
}

}
