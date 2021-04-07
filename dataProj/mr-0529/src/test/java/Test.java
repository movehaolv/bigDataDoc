/**
 * @Author: lvhao-004
 * @Version: 1.0
 * @Date: Create in 16:32 2020/12/26
 */


public class Test {
    public static void main(String[] args) {
        String[] a ="A\tB\tC\t".split("\t");
    }

}


abstract class A{
    void fun(){
        System.out.println(1);
    }
}

abstract class B extends A{
    @Override
    void fun(){
        System.out.println(2);
    }
}


class C {
    String name;

    public String setName(String name){
        name = name;
        return name;
    }

}




