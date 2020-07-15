package top.totoro.sql.clap;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 处理数据的缓存。
 * 避免过多的I/O
 *
 * @author dargon
 * @version 1.0
 */
public class SQLCache<Bean extends SQLBean> {
    // 缓存的数据，select时先从这里获取，不存在才去表中拿。
    private final Map<String, List<Bean>> CACHING = new ConcurrentHashMap<>();
    // 用于最久未使用规则的tableFilePath列表表，最久没被访问的tableFile会出现在列表的最后面，清除缓存时优先清除。
    private final LinkedList<String> LRU_KEYS = new LinkedList<>();
    // 允许缓存中bean数量的最大值
    private final int maxCachingSize = 0xfff;
    // 当前缓存中bean的数量
    private int currentCachingSize = 0;

    /**
     * 插入一条缓存数据。
     *
     * @param tableSubFilePath 插入数据的子表文件的路径
     * @param beanToCaching    要插入并缓存的数据
     * @return 插入并缓存成功
     */
    public boolean putToCaching(String tableSubFilePath, Bean beanToCaching) {
        refreshLRU(tableSubFilePath);
        List<Bean> cachingList = CACHING.computeIfAbsent(tableSubFilePath, key -> new ArrayList<>());
        if (cachingList.contains(beanToCaching)) return false;
        cachingList.add(beanToCaching);
        currentCachingSize++;
        Log.d("SQLCache", "putToCaching success bean = " + beanToCaching.getKey());
        return true;
    }

    /**
     * 插入一组缓存数据。只有缓存中不存在的数据才会被插入。
     *
     * @param tableSubFilePath 插入数据的子表文件的路径
     * @param listToCaching    要插入并缓存的所有数据
     * @return 插入并缓存成功
     */
    public boolean putToCaching(String tableSubFilePath, List<Bean> listToCaching) {
        assert listToCaching != null;
        refreshLRU(tableSubFilePath);
        List<Bean> cachingList = CACHING.get(tableSubFilePath);
        if (cachingList == null) {
            CACHING.put(tableSubFilePath, listToCaching);
            currentCachingSize += listToCaching.size();
        } else {
            for (Bean bean : listToCaching) {
                if (cachingList.contains(bean)) continue;
                cachingList.add(bean);
                currentCachingSize++;
            }
        }
        Log.d("SQLCache", "putToCaching list size = " + listToCaching.size());
        return true;
    }

    @Override
    public int hashCode() {
        return Objects.hash(maxCachingSize);
    }

    private void refreshLRU(String tableSubFilePath) {
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
                List<Bean> cachingList = CACHING.get(key);
                CACHING.remove(key);
                LRU_KEYS.remove(key);
                currentCachingSize -= cachingList.size();
            }
        }
    }

    public Bean getInCaching(String tableSubFilePath, String key) {
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

    public List<Bean> getInCaching(String tableSubFilePath) {
        List<Bean> caching = CACHING.get(tableSubFilePath);
        if (caching == null) return null;
        refreshLRU(tableSubFilePath);
        return caching;
    }

    public static void main(String[] args) {
        SQLCache cache = new SQLCache();
        for (int i = 0; i < 100; i++) {
            cache.putToCaching(i % 10 + "", new TestBean(i + ""));
            Log.d("SQLCache", "currentCachingSize = " + cache.currentCachingSize + ", INSERT_CACHING size = " + cache.CACHING.size());
        }

    }

    static class TestBean extends SQLBean {

        String key;

        public TestBean(String key) {
            this.key = key;
        }

        @Override
        void setKey(String key) {

        }

        @Override
        String getKey() {
            return key;
        }

        @Override
        boolean isSame(Object another) {
            if (another == this) return true;
            return key.equals(((SQLCache.TestBean) another).getKey());
        }
    }
}
