package io.playpen.plugin.mcbalancer;

public class BalanceTask implements Runnable {
    @Override
    public void run() {
        Balancer.balance();
    }
}
