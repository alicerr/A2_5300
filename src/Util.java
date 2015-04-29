
public abstract class Util {

	public static boolean retainEdgeByNodeID(double number){
		// compute filter parameters for netid shs257
		double fromNetID = 0.752; // 752 is 257 reversed
		double rejectMin = 0.9 * fromNetID;
		double rejectLimit = rejectMin + 0.01;
		return (number < rejectMin || number >= rejectLimit);
	}
	
	
	
	
	
	
}
