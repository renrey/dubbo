/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.dubbo.registry.dubbo;

import com.alibaba.dubbo.common.Constants;
import com.alibaba.dubbo.common.URL;
import com.alibaba.dubbo.common.bytecode.Wrapper;
import com.alibaba.dubbo.common.utils.NetUtils;
import com.alibaba.dubbo.common.utils.StringUtils;
import com.alibaba.dubbo.registry.Registry;
import com.alibaba.dubbo.registry.RegistryService;
import com.alibaba.dubbo.registry.integration.RegistryDirectory;
import com.alibaba.dubbo.registry.support.AbstractRegistryFactory;
import com.alibaba.dubbo.rpc.Invoker;
import com.alibaba.dubbo.rpc.Protocol;
import com.alibaba.dubbo.rpc.ProxyFactory;
import com.alibaba.dubbo.rpc.cluster.Cluster;
import com.alibaba.dubbo.rpc.proxy.javassist.JavassistProxyFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;

/**
 * DubboRegistryFactory
 *
 */
public class DubboRegistryFactory extends AbstractRegistryFactory {

    private Protocol protocol;
    private ProxyFactory proxyFactory;
    private Cluster cluster;

    private static URL getRegistryURL(URL url) {
        return url.setPath(RegistryService.class.getName())
                .removeParameter(Constants.EXPORT_KEY).removeParameter(Constants.REFER_KEY)
                .addParameter(Constants.INTERFACE_KEY, RegistryService.class.getName())
                .addParameter(Constants.CLUSTER_STICKY_KEY, "true")
                .addParameter(Constants.LAZY_CONNECT_KEY, "true")
                .addParameter(Constants.RECONNECT_KEY, "false")
                .addParameterIfAbsent(Constants.TIMEOUT_KEY, "10000")
                .addParameterIfAbsent(Constants.CALLBACK_INSTANCES_LIMIT_KEY, "10000")
                .addParameterIfAbsent(Constants.CONNECT_TIMEOUT_KEY, "10000")
                .addParameter(Constants.METHODS_KEY, StringUtils.join(new HashSet<String>(Arrays.asList(Wrapper.getWrapper(RegistryService.class).getDeclaredMethodNames())), ","))
                //.addParameter(Constants.STUB_KEY, RegistryServiceStub.class.getName())
                //.addParameter(Constants.STUB_EVENT_KEY, Boolean.TRUE.toString()) //for event dispatch
                //.addParameter(Constants.ON_DISCONNECT_KEY, "disconnect")
                .addParameter("subscribe.1.callback", "true")
                .addParameter("unsubscribe.1.callback", "false");
    }

    public void setProtocol(Protocol protocol) {
        this.protocol = protocol;
    }

    public void setProxyFactory(ProxyFactory proxyFactory) {
        this.proxyFactory = proxyFactory;
    }

    public void setCluster(Cluster cluster) {
        this.cluster = cluster;
    }

    // 创建注册中心对象
    @Override
    public Registry createRegistry(URL url) {
        url = getRegistryURL(url);// 协议url

        // urls就是所有注册中心url
        List<URL> urls = new ArrayList<URL>();
        urls.add(url.removeParameter(Constants.BACKUP_KEY));
        String backup = url.getParameter(Constants.BACKUP_KEY);
        // 如果注册中心url中有备用地址，把备用也加到urls集合中
        if (backup != null && backup.length() > 0) {
            String[] addresses = Constants.COMMA_SPLIT_PATTERN.split(backup);
            for (String address : addresses) {
                urls.add(url.setAddress(address));// 就是ip端口不同
            }
        }
        RegistryDirectory<RegistryService> directory = new RegistryDirectory<RegistryService>(RegistryService.class, url.addParameter(Constants.INTERFACE_KEY, RegistryService.class.getName()).addParameterAndEncoded(Constants.REFER_KEY, url.toParameterString()));

        // 集群中执行策略的invoker对象 -- cluster -》如failfast、广播，负责执行rpc请求的cluster策略
        /**
         * @see com.alibaba.dubbo.rpc.cluster.support.FailoverCluster#join(com.alibaba.dubbo.rpc.cluster.Directory)
         */
        Invoker<RegistryService> registryInvoker = cluster.join(directory);

        // 创建代理对象 -》负责做service接口对象的代理类，本地调用 根据接口方法转成rpc请求，通过registryInvoker发送
        /**
         * @see JavassistProxyFactory
         */
        RegistryService registryService = proxyFactory.getProxy(registryInvoker);

        // 实际对外使用的dubbo registry对象
        DubboRegistry registry = new DubboRegistry(registryInvoker, registryService);
        directory.setRegistry(registry);
        directory.setProtocol(protocol);

        // 根据（注册中心）urls 生成invoker
        directory.notify(urls);

        // 可知：/dubbo/com.alibaba.dubbo.registry.RegistryService 就是所有客户端的管理
        // 发起订阅 -》把当前客户端作为 消费者注册，path=RegistryService，参数为入参注册中心的url参数-- 即当前消费者的信息上报
        directory.subscribe(new URL(Constants.CONSUMER_PROTOCOL, NetUtils.getLocalHost(), 0, RegistryService.class.getName(), url.getParameters()));
        return registry;
    }
}
