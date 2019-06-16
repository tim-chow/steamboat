### 摘要

steamboat支持：

* 隔离请求
* 在请求大量失败时
  * 对后端进行熔断保护
  * 对失败请求进行降级处理

---

### 简介

在steamboat中有三个重要的组件：

* SteamBoat

  它是由若干个Cabin组成的。向SteamBoat中提交请求的时候，它会将请求转交给相应的Cabin。组成SteamBoat的多个Cabin之间是完全隔离的，SteamBoat是实现请求隔离的基础

* Cabin

  一个Cabin用来处理一类或多类请求。它是由如下的组件组成的：

  * Window

    Cabin中包含一个滚动窗口。当窗口处于开放状态时，它会统计总请求数量和失败请求数量，当失败率达到设定的阈值时，窗口就会进入到关闭状态。此时，所有提交给Cabin的请求都会被直接拒绝。也就是说，当窗口处于关闭状态时，Cabin会进行熔断保护。在过了关闭期之后，窗口会进入到半开状态。此时，一部分请求会被直接拒绝，另外一部分请求会被透给后端，当成功率达到设定的阈值时或过了半开期之后，窗口会重新打开；当失败率达到设定的阈值时，窗口会进入到关闭状态。它是实现熔断的基础

  * Executor

    用来执行提交给Cabin的任务。Executor是线程池、Tornado协程池等。Executor决定了Cabin并发处理请求的能力

* DegredationStrategy

  每个Cabin对应一个DegredationStrategy。当SteamBoat向Cabin提交请求失败时、请求被熔断时、请求处理过程中出现异常时，SteamBoat会调用该Cabin所对应的DegredationStrategy的相应方法，对请求进行降级处理，它实现降级的基础


