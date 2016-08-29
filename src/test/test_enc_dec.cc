#include "global/global_init.h"
#include "common/ceph_argparse.h"
#include "global/global_context.h"
#include "gtest/gtest.h"
#include "include/encoding.h"


TEST(test_enc_dec, vector) {
   vector<int> s,s2;
   
   s.push_back(1);
   s.push_back(2);
   s.push_back(3);
   bufferlist bl;
   encode(s, bl);
   std::cout << " Encoded length: " << bl.length() << std::endl;

   //EXPECT_EQ(s,s2);
}

int main(int argc, char **argv)
{
  vector<const char*> args;
  argv_to_vec(argc, (const char **)argv, args);

  global_init(NULL, args, CEPH_ENTITY_TYPE_CLIENT, CODE_ENVIRONMENT_UTILITY, 0); 
  common_init_finish(g_ceph_context);

  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

