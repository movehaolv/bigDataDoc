
/**
 * @Author: lvhao-004
 * @Version: 1.0
 * @Date: Create in 16:41 2021/7/14
 */


public class T {
    public static void main(String[] args) {
        Person p = new Stu();
        System.out.println(p.name);
        p.info();
    }
}

class Person{
    String name = "fu";

    public void info(){
        System.out.println("父类");
    }
}

class Stu extends Person{
    String name = "zi";

    public void info(){
        System.out.println("子类");
    }
}








