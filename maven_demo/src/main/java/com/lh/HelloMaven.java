package com.lh;

/**
 * @Author: lvhao-004
 * @Version: 1.0
 * @Date: Create in 9:43 2021/4/9
 */

// 以下命令需要在pom文件所在目录
// 1. clean - 2. compile/test-compile -
// 3. test 生成surefire-reports，保存测试报告
// 4. package 按照pom.xml配置把主程序打包生成jar或者war
// 5. install 本工程打包放入本地仓库
// 6. desploy  部署主程序（本工程打包，按照本工程的坐标保存到本地仓库，还会保存到私服仓库中，还会把项目部署到we b容器

// idea 集合maven
// 配置Maven Runner：
        //  -DarchetypeCatalog=internal（第一次创建后加入该参数，不然idea自身可能没有模板。作用：不下载初始化模板，避免网速过慢）
        //  选择jdk版本

/**
 * 依赖范围： 使用scope表示，compile，test，provided，默认是compile
 * maven构建项目过程有：编译 - 测试 - 打包 - 安装 - 部署
 * 如： junit在测试阶段，
 *    <dependency>
 *       <groupId>junit</groupId>
 *       <artifactId>junit</artifactId>
 *       <version>4.11</version>
 *       <scope>test</scope>
 *     </dependency>
 *
 *                  compile     test    provided
 * 对主程序是否有效     是           否       是
 * 对测试程序是否有效    是           是       是
 * 是否参与打包         是           否       否
 * 是否参与部署         是           否       否
 * **/



public class HelloMaven {
    public int add(int a, int b){
        return a + b;
    }

    public static void main(String[] args) {
        System.out.println("HelloMaven");
    }
}
