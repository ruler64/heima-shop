package com.hmall.item.constants;

public interface MQConstants {
    String LAZY_EXCHANGE_NAME = "item.lazy.direct";
    String LAZY_ITEM_ADD_QUEUE_NAME = "item.lazy.es.add.queue";
    String LAZY_ITEM_UPDATE_QUEUE_NAME = "item.lazy.es.update.queue";
    String LAZY_ITEM_DELETE_QUEUE_NAME = "item.lazy.es.delete.queue";
    String LAZY_ITEM_ADD_KEY = "lazy.item.add";
    String LAZY_ITEM_UPDATE_KEY = "lazy.item.update";
    String LAZY_ITEM_DELETE_KEY = "lazy.item.delete";
}
