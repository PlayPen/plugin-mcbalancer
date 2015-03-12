package net.thechunk.playpen.plugin.mcbalancer;

import lombok.Getter;
import lombok.extern.log4j.Log4j2;
import net.thechunk.playpen.coordinator.CoordinatorMode;
import net.thechunk.playpen.coordinator.PlayPen;
import net.thechunk.playpen.coordinator.network.Network;
import net.thechunk.playpen.plugin.AbstractPlugin;
import org.json.JSONArray;
import org.json.JSONObject;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

@Log4j2
public class MCBalancerPlugin extends AbstractPlugin {
    @Getter
    private static MCBalancerPlugin instance;

    @Getter
    private long scanRate;

    @Getter
    private int minPort;

    @Getter
    private int maxPort;

    @Getter
    private Map<String, ServerConfig> configs = new ConcurrentHashMap<>();

    public MCBalancerPlugin() {
        instance = this;
    }

    @Override
    public boolean onStart() {
        if(PlayPen.get().getCoordinatorMode() != CoordinatorMode.NETWORK) {
            log.fatal("Only network coordinators are supported");
            return false;
        }

        log.info("Loading configuration");
        scanRate = getConfig().getLong("scan-rate");
        minPort = getConfig().getInt("port-min");
        maxPort = getConfig().getInt("port-max");

        JSONArray servers = getConfig().getJSONArray("servers");
        for(int i = 0; i < servers.length(); ++i) {
            JSONObject obj = servers.getJSONObject(i);
            ServerConfig config = new ServerConfig();
            config.setPackageId(obj.getString("package"));
            config.setPrefix(obj.getString("prefix"));
            config.setTargetRatio(obj.getDouble("ratio"));
            config.setMinServers(obj.getInt("min"));
            config.setMaxServers(obj.getInt("max"));

            if(configs.containsKey(config.getPackageId())) {
                log.fatal("Cannot register multiple server types with the same package");
                return false;
            }

            configs.put(config.getPackageId(), config);
        }

        log.info("Starting balancer with scan rate of " + scanRate);
        log.info("Initial balance will start in " + scanRate + " seconds");

        Network.get().getScheduler().scheduleAtFixedRate(new BalanceTask(), scanRate, scanRate, TimeUnit.SECONDS);

        return true;
    }

    @Override
    public void onStop() {
    }
}
