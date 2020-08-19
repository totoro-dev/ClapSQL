package top.totoro.sql.clap;

import com.google.gson.Gson;
import top.totoro.sql.clap.uitl.Log;

import java.io.*;
import java.lang.reflect.Type;
import java.nio.charset.StandardCharsets;
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
    private Map<String, List<Bean>> CACHING = new ConcurrentHashMap<>();
    // 用于最久未使用规则的tableFilePath列表表，最久没被访问的tableFile会出现在列表的最后面，清除缓存时优先清除。
    private final LinkedList<String> LRU_KEYS = new LinkedList<>();
    // 允许缓存中bean数量的最大值 16384
    private final int maxCachingSize = 0x3fff;
    // 当前缓存中bean的数量
    private int currentCachingSize = 0;
    // 持久化缓存的路径
    private final String persistentCacheRootPath
            = System.getProperty("java.io.tmpdir")
            + File.separator
            + "clap_db"
            + File.separator;
    private final String persistentCacheFileName = "cache.json";
    private File cacheFile = new File(persistentCacheRootPath + persistentCacheFileName);

    /* changed by dragon 2020/07/18 取消不必要的SQLService，解决循环依赖 */
//    public SQLCache(SQLService<Bean> sqlService) {
//        this.sqlService = sqlService;
//    }

    /**
     * 注册持久化缓存
     * 当退出程序时被自动执行
     */
    @SuppressWarnings("ResultOfMethodCallIgnored")
    private void registerPersistentCache() {
        // 退出JVM时处理缓存持久化
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            Log.d(this, "registerPersistentCache() current caching size = " + currentCachingSize);
            if (!cacheFile.getParentFile().exists()) {
                cacheFile.getParentFile().mkdirs();
            }
            try {
                if (!cacheFile.exists()) {
                    cacheFile.createNewFile();
                }
                Gson gson = new Gson();
                StringBuilder caching = new StringBuilder();
                CACHING.forEach((file, beans) -> {
                    if (beans.isEmpty()) return;
                    // 一行文件路径，一行数据
                    // C:\Users\dragon\AppData\Local\Temp\clap_db\top.totoro.sql.clap.test\db\test1\63.tab
                    // [{"name":"default","key":"17"},{"name":"default","key":"144"},{"name":"default","key":"204"}]
                    caching.append(file).append(System.getProperty("line.separator", "\n"));
                    caching.append(gson.toJson(beans)).append(System.getProperty("line.separator", "\n"));
                });
                OutputStreamWriter osw = new OutputStreamWriter(new FileOutputStream(cacheFile, false), StandardCharsets.UTF_8);
                osw.write(caching.toString());
                osw.flush();
                osw.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }));
    }

    /**
     * 加载持久化了的缓存
     * 从系统中加载上一次退出时的缓存内容
     *
     * @param dbPath   加载的数据库路径
     * @param beanType 需要加载的bean类型
     */
    @SuppressWarnings("ResultOfMethodCallIgnored")
    protected void loadPersistentCache(String dbPath, Type beanType) {
        long startTime = new Date().getTime();
        cacheFile = new File(dbPath + File.separator + persistentCacheFileName);
        if (!cacheFile.getParentFile().exists()) {
            cacheFile.getParentFile().mkdirs();
        }
        FileReader reader = null;
        BufferedReader bufferedReader = null;
        try {
            if (!cacheFile.exists()) {
                cacheFile.createNewFile();
            }
            reader = new FileReader(cacheFile);
            bufferedReader = new BufferedReader(reader);
            String line;
            Gson gson = new Gson();
            int lineNum = 0;
            String filePath = "";
            // 一行一行的读取数据
            while ((line = bufferedReader.readLine()) != null) {
                if (lineNum % 2 == 0) {
                    filePath = line;
                } else {
                    line = line.substring(1, line.length() - 1);
                    String[] parts = line.split(",\\{");
                    LinkedList<Bean> list = new LinkedList<>();
                    for (int i = 0; i < parts.length; i++) {
                        String part = parts[i];
                        if (i != 0) part = "{" + part;
                        part = part.trim();
                        list.add(gson.fromJson(part, beanType));
                        currentCachingSize++;
                    }
                    LRU_KEYS.add(filePath);
                    CACHING.put(filePath, list);
                }
                lineNum++;
            }
            if (CACHING == null) CACHING = new ConcurrentHashMap<>();
            Log.d(this, "loadPersistentCache() current caching size = " + currentCachingSize);
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                if (reader != null) {
                    reader.close();
                }
                if (bufferedReader != null) {
                    bufferedReader.close();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
            Log.d(this, "loadPersistentCache() cost time = " + (new Date().getTime() - startTime) + "ms");
        }
    }

    public SQLCache() {
//        loadPersistentCache();
        registerPersistentCache();
    }

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
                CACHING.remove(key);
                LRU_KEYS.remove(key);
                if (cachingList == null) continue;
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
