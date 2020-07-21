package top.totoro.sql.clap;

import top.totoro.sql.clap.uitl.Log;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 处理数据的缓存，避免过多的I/O，使用的算法是LRU最近最久未使用算法。
 *
 * @author dargon
 * @version 1.0
 */
public class SQLCache<Bean extends SQLBean> {
    // 缓存的数据，select时先从这里获取，不存在才去表中拿。
    private final Map<String, List<Bean>> CACHING = new ConcurrentHashMap<>();
    // 用于最久未使用规则的tableFilePath列表表，最久没被访问的tableFile会出现在列表的最后面，清除缓存时优先清除。
    private final LinkedList<String> LRU_KEYS = new LinkedList<>();
    // 允许缓存中bean数量的最大值 65536
    private final int maxCachingSize = 0xf0000;
    // 当前缓存中bean的数量
    private int currentCachingSize = 0;

    /* changed by dragon 2020/07/18 取消不必要的SQLService，解决循环依赖 */
//    public SQLCache(SQLService<Bean> sqlService) {
//        this.sqlService = sqlService;
//    }

    /**
     * 插入一条缓存数据。
     *
     * @param tableSubFilePath 插入数据的子表文件的路径
     * @param beanToCaching    要插入并缓存的数据
     * @return 插入并缓存成功
     */
    protected boolean putToCaching(String tableSubFilePath, Bean beanToCaching) {
        refreshLRU(tableSubFilePath);
        List<Bean> cachingList = CACHING.computeIfAbsent(tableSubFilePath, key -> new ArrayList<>());
        if (cachingList.contains(beanToCaching)) return false;
        cachingList.add(beanToCaching);
        currentCachingSize++;
//        Log.d("SQLCache", "putToCaching success bean = " + beanToCaching.getKey());
        return true;
    }

    /**
     * 插入一组缓存数据。只有缓存中不存在的数据才会被插入。
     *
     * @param tableSubFilePath 插入数据的子表文件的路径
     * @param listToCaching    要插入并缓存的所有数据
     * @return 插入并缓存成功
     */
    protected boolean putToCaching(String tableSubFilePath, List<Bean> listToCaching) {
        assert listToCaching != null && tableSubFilePath != null;
        refreshLRU(tableSubFilePath);
        List<Bean> cachingList = CACHING.get(tableSubFilePath);
        if (cachingList == null) {
            CACHING.put(tableSubFilePath, listToCaching);
        } else {
//            for (Bean bean : listToCaching) {
//                if (cachingList.contains(bean)) continue;
//                cachingList.add(bean);
//                currentCachingSize++;
//            }
            currentCachingSize -= cachingList.size();
            CACHING.replace(tableSubFilePath, listToCaching);
        }
        currentCachingSize += listToCaching.size();
//        Log.d("SQLCache", "putToCaching list size = " + listToCaching.size());
        return true;
    }

    private synchronized void refreshLRU(String tableSubFilePath) {
        LRU_KEYS.remove(tableSubFilePath);
        // 将当前的tableFilePath放到最前的位置，表示是最近使用的
        LRU_KEYS.add(0, tableSubFilePath);
        resizeInsertCachingMap();
    }

    /**
     * 刷新缓存，清除最久未被访问的缓存
     */
    private void resizeInsertCachingMap() {
        if (currentCachingSize > maxCachingSize) {
            LinkedList<String> copyKeys = new LinkedList<>(LRU_KEYS);
            for (int i = copyKeys.size() - 1; i >= 0 && currentCachingSize > maxCachingSize; i--) {
                String key = copyKeys.get(i);
                if (key == null) continue;
                List<Bean> cachingList = CACHING.get(key);
                if (cachingList == null) continue;
                CACHING.remove(key);
                LRU_KEYS.remove(key);
                // 这里将缓存中的数据刷新到表文件中，确保数据已在缓存中但是还没有写入表文件的情况下表文件是稳定的。
//                sqlService.refreshTable(new File(key), cachingList);
                currentCachingSize -= cachingList.size();
            }
        }
    }

    protected Bean getInCaching(String tableSubFilePath, String key) {
        List<Bean> caching = getInCaching(tableSubFilePath);
        if (caching == null) return null;
        for (Bean bean : caching) {
            if (key.equals(bean.getKey())) {
                Log.d("SQLCache", "get from caching success key = " + key);
                return bean;
            }
        }
        return null;
    }

    protected List<Bean> getInCaching(String tableSubFilePath) {
        List<Bean> caching = CACHING.get(tableSubFilePath);
        if (caching == null) return null;
        refreshLRU(tableSubFilePath);
        return caching;
    }
}
