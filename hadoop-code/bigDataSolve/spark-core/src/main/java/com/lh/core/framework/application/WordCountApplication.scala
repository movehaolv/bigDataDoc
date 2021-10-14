package com.lh.core.framework.application

import com.lh.core.framework.common.TApplication
import com.lh.core.framework.controller.WordCountController

object WordCountApplication extends App with TApplication{

    // 启动应用程序
    start(){
        val controller = new WordCountController()
        controller.dispatch()
    }

}
