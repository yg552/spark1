/**
 * author 杨广
 * 2022/4/15 0015
 */
public class Test {
    //continue跳过本次循环，后面的语句不执行
    public static void main(String[]args){
        for(int i=0;i<10;i++){
            if(i==5){
                continue;
            }
            System.out.println(i);
        }
    }
}

class AA<T>{
    public T getdata(T data){
        return data;
    }
}