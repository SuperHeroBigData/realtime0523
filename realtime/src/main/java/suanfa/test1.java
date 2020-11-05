package suanfa;/*
@author zilong-pan
@creat 2020-10-26 20:54
@desc $
*/

import scala.Int;

import javax.swing.tree.TreeNode;
import java.util.HashMap;
import java.util.Stack;

public class test1 {
    public static void main(String[] args) {
       /* int[] ints = {1, 2, 3, 4, 5, 6};
        int[] gettwo = new test1().gettwo(ints, 199);
        for (int i = 0; i < gettwo.length; i++) {
            System.out.println(gettwo[i]);
        }*/
       /* String a = "fbx";
        String b = "fbs";
        System.out.println(new test1().aparseb(a, b));*/
        Thread thread = new Thread();
        thread.run();



    }
    public int[] gettwo(int[] ints,int target)
    {
        HashMap<Integer, Integer> map = new HashMap<Integer, Integer>();
        for (int i = 0; i < ints.length; i++) {
            int complement=target-ints[i];
            if(map.containsKey(complement))
            {
                return new int[] { map.get(complement), i };
            }
            map.put(ints[i],i);
        }
        throw new RuntimeException("数组中不存在两数之和为target的数");
    }
    /*
    1->1
    2->2
    3->3
    4->5
     */
    public int getcli(int n)
    {
        if(n==1)
            return 1;
        int[] ints = new int[n + 1];
        ints[1]=1;
        ints[2]=2;
        for (int i = 3; i <=ints.length ; i++) {
            ints[i]=ints[i-1]+ints[i-2];
        }
        return ints[n];
    }
   /* public TreeNode invertTree(TreeNode root)
    {
        if(root==null)
        {
            return null;
        }
        root
    }*/
   //给定一个字符串 s，找到 s 中最长的回文子串。你可以假设 s 的最大长度为 1000
    public  int  findCenter(String s,int right,int left)
    {
        //找中心，寻找回文字符串对应的长度
        while(right<s.length()&&left>=0&&s.charAt(right)==s.charAt(left))
        {
            right++;
            left--;
        }
        return right-left-1;
    }
    //对字符串进行处理，找到最长回文字串
    public String getDesString(String s)
    {
        test1 test1 = new test1();
        int start =0;
        int end=0;
        for (int i = 0; i <s.length() ; i++) {
            int len1=test1.findCenter(s,i,i);
            int len2=test1.findCenter(s,i,i+1);
            int len=Math.max(len1,len2);
            if(len>(end-start))
            {
                end=i+len/2;
                start=i-(len-1)/2;
            }
        }
    return s.substring(start,end);
    }
    // 有效的括号
    //符号成对出现，采用stack+hashmap如()[]{}
    public boolean validBracket(String s)
    {

        Stack<Character> stack = new Stack<>();
        for (int i = 0; i < s.length(); i++) {
            if(s.charAt(i)=='(') stack.push(')');
            else if (s.charAt(i)=='{') stack.push('}');
            else if (s.charAt(i)=='[') stack.push(']');
            else if (stack.isEmpty()||stack.pop()!=s.charAt(i))
                return false;
        }
        return stack.isEmpty();
    }
    //数组中最大的第k个元素
    //进行快速选择排序，后取第k个元素




    //字符串a（fbs）->字符串b（fbx）最小转换次数，如插入、删除、更新算为一次操作；
    public int aparseb(String a,String b)
    {
        char[] array = a.toCharArray();
        char[] array1 = b.toCharArray();
        int result=0;
        if(a.length()==0)
        {
            return b.length();
        }
        if(b.length()==0)
        {
            return a.length();
        }
        if(array[array.length>0?array.length-1:0]==array1[array1.length>0?array1.length-1:0])
        {
            result+=aparseb(a.substring(0,a.length()==0?0:a.length()-1),b.substring(0,b.length()==0?0:b.length()-1));
        }else
        {
            //分三种情况
            //1、a的最后一个变化删除
            int total=aparseb(a.substring(0,a.length()==0?0:a.length()-1),b);
            //2、b的最后一个变化删除
            int total1 = aparseb(a, b.substring(0, b.length()==0?0:b.length()-1)) ;
            //3、a、b同时变化
            int total2 = aparseb(a.substring(0,a.length()==0?0:a.length()-1),b.substring(0,b.length()==0?0:b.length()-1)) ;
            result+= (total <= total1 ? (total <= total2 ? total : total2) : ((total1 <=total2) ? total1 : total2))+1;
        }
        return result;

    }


}
