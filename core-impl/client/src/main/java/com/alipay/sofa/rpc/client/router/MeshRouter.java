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
package com.alipay.sofa.rpc.client.router;

import com.alipay.sofa.rpc.bootstrap.ConsumerBootstrap;
import com.alipay.sofa.rpc.client.*;
import com.alipay.sofa.rpc.common.RpcConstants;
import com.alipay.sofa.rpc.common.utils.StringUtils;
import com.alipay.sofa.rpc.config.ConsumerConfig;
import com.alipay.sofa.rpc.config.RegistryConfig;
import com.alipay.sofa.rpc.core.request.SofaRequest;
import com.alipay.sofa.rpc.ext.Extension;
import com.alipay.sofa.rpc.filter.AutoActive;
import com.alipay.sofa.rpc.log.Logger;
import com.alipay.sofa.rpc.log.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * 从mesh获取路由,只为 bolt 开启。
 * <p>
 *
 * @author <a href="mailto:zhiyuan.lzy@antfin.com">zhiyuan.lzy</a>
 */
@Extension(value = "mesh", order = -19000)
@AutoActive(consumerSide = true)
public class MeshRouter extends Router {
    private final static Logger LOGGER          = LoggerFactory.getLogger(MeshRouter.class);
    /**
     * 路由路径：注册中心
     *
     * @since 5.5.0
     */
    public static final String  RPC_MESH_ROUTER = "MESH";

    /**
     * 服务消费者配置
     */
    protected ConsumerBootstrap consumerBootstrap;

    @Override
    public void init(ConsumerBootstrap consumerBootstrap) {
        this.consumerBootstrap = consumerBootstrap;
    }

    @Override
    public boolean needToLoad(ConsumerBootstrap consumerBootstrap) {
        ConsumerConfig consumerConfig = consumerBootstrap.getConsumerConfig();
        // 不是直连，mesh mode
        final boolean isDirect = StringUtils.isNotBlank(consumerConfig.getDirectUrl());
        final boolean meshMode = consumerConfig.isMeshMode();
        LOGGER.info("mesh mode: " + meshMode);
        return !isDirect && meshMode == true;
    }

    @Override
    public List<ProviderInfo> route(SofaRequest request, List<ProviderInfo> providerInfos) {
        List<ProviderInfo> meshProviderInfos = new ArrayList<ProviderInfo>();
        AddressHolder addressHolder = consumerBootstrap.getCluster().getAddressHolder();
        ConnectionHolder connectionHolder = consumerBootstrap.getCluster().getConnectionHolder();
        if (addressHolder != null && connectionHolder != null) {
            ProviderInfo tmpProviderInfo = null;
            List<ProviderInfo> current = addressHolder.getProviderInfos(RpcConstants.ADDRESS_MESH_GROUP);
            List<ProviderInfo> registries = addressHolder.getProviderInfos(RpcConstants.ADDRESS_DEFAULT_GROUP);
            LOGGER.info("current size: " + current.size());
            LOGGER.info("mesh mode runtime: " + consumerBootstrap.getConsumerConfig().isMeshMode());
            if (!current.isEmpty()) {
                if (consumerBootstrap.getConsumerConfig().isMeshMode() != true) {
                    meshProviderInfos.addAll(registries);
                } else {
                    meshProviderInfos.addAll(current);
                }
            } else {
                if (providerInfos != null) {
                    LOGGER.info("providerInfos no null");
                    tmpProviderInfo = providerInfos.get(0);
                } else {
                    if (registries.size() > 0) {
                        LOGGER.info("registries 0: " + registries.get(0));
                        tmpProviderInfo = registries.get(0);
                    }
                }
                if (tmpProviderInfo != null) {
                    LOGGER.info("ProviderInfo host1: " + tmpProviderInfo.getHost());
                    ProviderInfo providerInfo = new ProviderInfo(request.getInterfaceName() + ".rpc",
                        tmpProviderInfo.getPort());
                    meshProviderInfos.add(providerInfo);
                    LOGGER.info("ProviderInfo host2: " + providerInfo.getHost());
                    ProviderGroup providerGroup = new ProviderGroup(RpcConstants.ADDRESS_MESH_GROUP);
                    providerGroup.setProviderInfos(meshProviderInfos);
                    if (addressHolder.getProviderGroup(RpcConstants.ADDRESS_MESH_GROUP) == null) {
                        addressHolder.addProvider(providerGroup);
                    }
                    if (connectionHolder.getAvailableClientTransport(meshProviderInfos.get(0)) == null) {
                        connectionHolder.addProvider(providerGroup);
                    }
                }
            }
        }
        recordRouterWay(RPC_MESH_ROUTER);
        return meshProviderInfos;
    }

}
