import com.BlogApplication;
import com.common.db.BizDB;
import com.common.db.IDbOp;
import com.common.util.ConfigurationPropertiesConfig;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import java.sql.Connection;
import java.util.List;
import java.util.Map;

/**
 * @Author wul
 * @Description
 * @Date 2019/12/11 15:24
 */
@RunWith(value = SpringRunner.class)
@SpringBootTest(classes = BlogApplication.class)
public class DbTest {

    @Autowired
    BizDB bizDb;

    @Autowired
    private ConfigurationPropertiesConfig configurationPropertiesConfig;


    @Test
    public void test1(){
        try {
            final Connection conn = bizDb.getConn(false);
            final IDbOp optimizeDbOp = bizDb.getOptimizeDbOp();
            final List<Map<String, Object>> user = bizDb.searchAsMapList("tb_usap_user_cert");
            System.out.println(configurationPropertiesConfig.toString());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
