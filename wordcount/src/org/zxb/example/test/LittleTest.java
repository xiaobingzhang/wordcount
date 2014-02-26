package org.zxb.example.test;

public class LittleTest {
	public static void main(String[] args) {
		System.out.println(Integer.valueOf("127") == Integer.valueOf("127"));// true
		/*
		 * valueOf会返回一个Integer（整型）对象，当被处理的字符串在-128和127（包含边界） 之间时，返回的对象是预先缓存的。
		 */
		System.out.println("---------");
		System.out.println(Integer.valueOf("129") == Integer.valueOf("129"));// false
		/*
		 * 此处是相对比较难以理解的地方----------
		 * 首先进行了字符串解析，然后如果解析的值位于-128和127之间，就会从静态缓存中返回对象。如果超出了这个范围，
		 * 就会调用Integer()方法并将解析的值作为参数传入，得到一个新的对象。
		 * 
		 * 
		 * 上面的表达式返回false，因为128没有存在静态缓冲区。所以每次在判断相等时等式两边都会创建新的Integer对象。
		 * 由于两个Integer对象不同，所以==只有等式两边代表同一个对象时才会返回true。因此，上面的等式返回false。
		 */
		System.out.println(Integer.parseInt("129") == Integer.valueOf("129"));// true
		// 此处为int和Integer之间的比较是没有意义的，
		// 所以Java在进行比较之前会讲Integer自动拆箱，所以最后进行的比较是int和int值之间的比较，所以为true
		/*
		 * 一个 unboxing conversion
		 * （一种比较时的转换，把对对象的引用转换为其对应的原子类型）在第三行的比较中发生了。因为比较操作符使用了
		 * ==同时等号的两边存在一个int型和一个Integer对象的引用。
		 * 这样的话，等号右边返回的Integer对象被进一步转换成了int数值，才与左边进行相等判断。
		 */
		System.out.println("---------------");
		int i1 = 1111;
		Integer i = new Integer(1111);
		Integer j = new Integer(1111);

		System.out.println(i == j);// false
		System.out.println(i1 == i);// true
	}
}
