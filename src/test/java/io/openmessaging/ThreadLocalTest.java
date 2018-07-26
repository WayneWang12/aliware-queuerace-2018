package io.openmessaging;

public class ThreadLocalTest {

    public static void main(String[] args) {
        ThreadLocal<Integer> integerThreadLocal = new ThreadLocal<Integer>(){
            @Override
            protected Integer initialValue() {
                return 1;
            }
        };

        System.out.println(integerThreadLocal.get());
    }

}
