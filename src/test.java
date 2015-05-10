import org.apache.hadoop.io.Text;


public class test {

	public static void main(String[] args) {
		double p = 685230.;
		double n = 1/p;
		System.out.println(n);
		System.out.println(Double.parseDouble(new Text(n + "").toString()));

	}

}
