# Concurrency

Concurrency in actions can improve container reuse, and may be beneficial in cases where:

* your action can tolerate multiple activations being processed at once
* you can rely on external log collection (e.g. via docker log drivers or some decoupled collection process like fluentd)

## Enabling

Concurrent activation processing within the same action container can be enabled by:

* enable the akka http client at invoker config
  * e.g. CONFIG_whisk_containerPool_akkaClient=true
* use a kind that supports concurrency (currently only nodejs:6 and nodejs:8)
* enable concurrency at runtime container env (nodejs container only allows concurrency when started with an env var __OW_ALLOW_CONCURRENT=true)
  * e.g. CONFIG_whisk_containerFactory_containerArgs_extraArgs_env_0="__OW_ALLOW_CONCURRENT=true"
* disable log collection at invoker
  * e.g. CONFIG_whisk_spi_LogStoreProvider="whisk.core.containerpool.logging.LogDriverLogStoreProvider"
* (optional) enable alternate log retrieve at controller
  * If you want OW api /activations/\<id\>/logs to return logs, you need to have an alternate log collection mechanism for action containers
  * e.g. CONFIG_whisk_spi_LogStoreProvider=whisk.core.containerpool.logging.SplunkLogStoreProvider
* set the concurrency limit > 1 on any action that you want to process activations concurrently.


## Known issues 

Known issues with enabling concurrent activation processing are: 

* Due to how activations are sent from Controller to Invoker, the only way to process n activations (where n > number of containers allowed in the Invoker) is to read additional messages from Kafka before they might be able to be processed. 
This means that these messages will be buffered in memory until a container is available to process them. If these activations are for actions that tolerate concurrency, and there is a warm container with enough concurrency-capacity to process them, they will be processed immediately. 
If the activations are for actions that do NOT support concurrency, or there are no containers available, the messages will remain in memory until containers become available. As such, the variety of action configurations may affect how many messages are buffered in memory at any time, and if a crash occurred those messages would be lost.  
  
* Indirectly related to concurrency, since log collection must be decoupled, the logs are not stored with the resulting activation entity. This causes these different API behaviors:
  * `wsk activation get` and `wsk activation poll` does not return logs for primitive actions (only returns logs for sequences/compositions)
  * `wsk activation logs` does not return logs for sequences/compositions (only returns logs for primitive actions)