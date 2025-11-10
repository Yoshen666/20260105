from xinxiang import config


def create_temp_table(duck_db):
    table_type = ""
    if not config.g_debug_mode:
        table_type = 'TEMP'
    sql = """
    create {table_type} table APS_TMP_ETL_RLS_BOAT_CONSTRAINTS
    (
      eqp_id         VARCHAR(64),
      prodg1         VARCHAR(64),
      tech           VARCHAR(64),
      ope_no         VARCHAR(64),
      ch_set         VARCHAR(64),
      other_min_ctrl DECIMAL,
      prod_id        VARCHAR(64)
    )
    """.format(table_type=table_type)
    duck_db.sql(sql)
    sql = """
    create {table_type} table APS_TMP_ETL_RLS_BOAT_CONSTRAINTS2
    (
      eqp_id   VARCHAR(64),
      foup_num DECIMAL,
      ch_set   VARCHAR(64)
    )
    """.format(table_type=table_type)
    duck_db.sql(sql)
    sql = """ 
    create {table_type} table APS_TMP_ETL_RLS_BOAT_CONSTRAINTS3
    (
      eqp_id   VARCHAR(64),
      foup_num DECIMAL,
      ch_set   VARCHAR(64)
    )
    """.format(table_type=table_type)
    duck_db.sql(sql)
    sql = """ 
    create {table_type} table APS_TMP_ETL_RLS_BATCH_INFO
    (
      batch_name  VARCHAR(64),
      mainpd_id   VARCHAR(64),
      ope_no      VARCHAR(64),
      prodspec_id VARCHAR(64)
    )
    """.format(table_type=table_type)
    duck_db.sql(sql)
    sql = """
    create {table_type} table APS_TMP_ETL_RLS_MULTI_LOT
    (
      lot_id  VARCHAR(64),
      cast_id VARCHAR(64),
      ope_no  VARCHAR(64)
    )
    """.format(table_type=table_type)
    duck_db.sql(sql)
    sql = """
    create {table_type} table APS_TMP_ETL_RLS_APC_INHIBIT_LOT
    (
      eqp_id          VARCHAR(64),
      physical_recipe VARCHAR(64)
    )
    """.format(table_type=table_type)
    duck_db.sql(sql)
    sql = """
    create {table_type} table APS_TMP_ETL_RLS_LOT_TO_EQPG
    (
      toolg_id     VARCHAR(64),
      lot_id       VARCHAR(64),
      ope_no       VARCHAR(64),
      prod_id      VARCHAR(64),
      plan_id      VARCHAR(64),
      csfr_plan_id VARCHAR(64)
    )
    """.format(table_type=table_type)
    duck_db.sql(sql)
    sql = """ 
    create {table_type} table APS_TMP_ETL_RLS_LOTHISTORY
    (
      lot_id VARCHAR(64),
      eqp_id VARCHAR(64),
      ope_no VARCHAR(64)
    )
    """.format(table_type=table_type)
    duck_db.sql(sql)
    sql = """
    create {table_type} table APS_TMP_ETL_RLS_RESOURCE_FLAG
    (
      lot_id  VARCHAR(64),
      eqp_id  VARCHAR(64),
      ope_no  VARCHAR(64),
      plan_id VARCHAR(64),
      prod_id VARCHAR(64)
    )
    """.format(table_type=table_type)
    duck_db.sql(sql)
    sql = """
    create {table_type} table APS_TMP_ETL_RLS_BATCH_OPE
    (
      mainpd_id VARCHAR(60),
      ope_no    VARCHAR(60)
    )
    """.format(table_type=table_type)
    duck_db.sql(sql)
    sql = """ 
    create {table_type} table APS_TMP_ETL_RLS_WIP
    (
      parent_id          VARCHAR(64),
      lot_id             VARCHAR(64),
      prodspec_id        VARCHAR(64),
      plan_id            VARCHAR(64),
      target_plan_id     VARCHAR(64),
      ope_no             VARCHAR(64),
      ope_seq            VARCHAR(64),
      target_ope_no      VARCHAR(64),
      target_ope_seq     VARCHAR(64),
      target_toolg_id    VARCHAR(64),
      lot_type           VARCHAR(64),
      lot_finished_state VARCHAR(64),
      foup_id            VARCHAR(64),
      wafer_qty          DECIMAL,
      lot_status         VARCHAR(64),
      flow_recipe        VARCHAR(64),
      create_time        VARCHAR(64),
      trans_state        VARCHAR(64),
      batch_id           VARCHAR(64),
      sub_lot_type       VARCHAR(64)
    )
    """.format(table_type=table_type)
    duck_db.sql(sql)
    sql = """ 
        create {table_type} table APS_TMP_ETL_RLS_WIP_V1
        (
          parent_id          VARCHAR(64),
          lot_id             VARCHAR(64),
          prodspec_id        VARCHAR(64),
          plan_id            VARCHAR(64),
          target_plan_id     VARCHAR(64),
          ope_no             VARCHAR(64),
          ope_seq            VARCHAR(64),
          target_ope_no      VARCHAR(64),
          target_ope_seq     VARCHAR(64),
          target_toolg_id    VARCHAR(64),
          lot_type           VARCHAR(64),
          lot_finished_state VARCHAR(64),
          foup_id            VARCHAR(64),
          wafer_qty          DECIMAL,
          lot_status         VARCHAR(64),
          flow_recipe        VARCHAR(64),
          create_time        VARCHAR(64),
          trans_state        VARCHAR(64),
          batch_id           VARCHAR(64),
          sub_lot_type       VARCHAR(64)
        )
        """.format(table_type=table_type)
    duck_db.sql(sql)
    sql = """
    create {table_type} table APS_TMP_ETL_RLS_WIP_V
    (
      lot_id                  VARCHAR(64),
      prodspec_id             VARCHAR(64),
      mainpd_id               VARCHAR(64),
      ope_no                  VARCHAR(64),
      lot_type                VARCHAR(64),
      lot_finished_state      VARCHAR(64),
      cassete_id              VARCHAR(64),
      wafer_qty               DECIMAL,
      priority_class          VARCHAR(64),
      lot_proc_state          VARCHAR(64),
      last_ope_comp_time      VARCHAR(64),
      eqp_id                  VARCHAR(64),
      ope_no_start_time       VARCHAR(64),
      last_process_start_time VARCHAR(64),
      tech_id                 VARCHAR(64),
      customer_id             VARCHAR(64),
      parent_lot_id           VARCHAR(64),
      trans_state             VARCHAR(64),
      batch_id                VARCHAR(64),
      sub_lot_type            VARCHAR(64)
    )
    """.format(table_type=table_type)
    duck_db.sql(sql)
    sql = """ 
    create {table_type} table APS_TMP_ETL_RLS_DYB_RETURN_LOT
    (
      lot_id                  VARCHAR(64),
      prodspec_id             VARCHAR(64),
      current_ope_no          VARCHAR(64),
      openo                   VARCHAR(64),
      dyb_mainpd_id           VARCHAR(64),
      return_mainpd_id        VARCHAR(64),
      return_ope_no           VARCHAR(64),
      lot_type                VARCHAR(64),
      cassete_id              VARCHAR(64),
      wafer_qty               DECIMAL,
      priority_class          VARCHAR(60),
      lot_proc_state          VARCHAR(64),
      last_ope_comp_time      VARCHAR(64),
      eqp_id                  VARCHAR(64),
      ope_no_start_time       VARCHAR(64),
      last_process_start_time VARCHAR(64),
      tech_id                 VARCHAR(64),
      customer_id             VARCHAR(64),
      parent_lot_id           VARCHAR(60),
      trans_state             VARCHAR(64),
      batch_id                VARCHAR(64),
      sub_lot_type            VARCHAR(64)
    )
    """.format(table_type=table_type)
    duck_db.sql(sql)
    sql = """ 
    create {table_type} table APS_TMP_ETL_RLS_DYB_LOT
    (
      lot_id                  VARCHAR(64),
      prodspec_id             VARCHAR(64),
      current_ope_no          VARCHAR(64),
      openo                   VARCHAR(64),
      dyb_mainpd_id           VARCHAR(64),
      return_mainpd_id        VARCHAR(64),
      return_ope_no           VARCHAR(64),
      lot_type                VARCHAR(64),
      cassete_id              VARCHAR(64),
      wafer_qty               DECIMAL,
      priority_class          VARCHAR(64),
      lot_proc_state          VARCHAR(64),
      last_ope_comp_time      VARCHAR(64),
      eqp_id                  VARCHAR(64),
      ope_no_start_time       VARCHAR(64),
      last_process_start_time VARCHAR(64),
      tech_id                 VARCHAR(64),
      customer_id             VARCHAR(64),
      parent_lot_id           VARCHAR(64),
      pos_pdid                VARCHAR(64),
      step_id                 VARCHAR(64),
      eqp_g                   VARCHAR(64),
      prodg_id                VARCHAR(64),
      prodg_tech              VARCHAR(64),
      recipe                  VARCHAR(64),
      toolg_type              VARCHAR(64),
      trans_state             VARCHAR(64),
      batch_id                VARCHAR(64),
      sub_lot_type            VARCHAR(64),
      batch_name              VARCHAR(64),
        m_recipe                VARCHAR(128),
        physical_recipe_id      VARCHAR(64),
        recipe_id_c             VARCHAR(64),
        lcrecipe_id             VARCHAR(64),
        max_batch_size          VARCHAR(64)
    )
    """.format(table_type=table_type)
    duck_db.sql(sql)
    sql = """ 
    create {table_type} table APS_TMP_ETL_RLS_DYB_LOT_PO_N1
    (
      lot_id                  VARCHAR(64),
      prodspec_id             VARCHAR(64),
      current_ope_no          VARCHAR(64),
      dyb_mainpd_id           VARCHAR(64),
      pos_pdid                VARCHAR(64),
      ope_no                  VARCHAR(64),
      ope_seq                 VARCHAR(64),
      return_mainpd_id        VARCHAR(64),
      return_ope_no           VARCHAR(64),
      lot_type                VARCHAR(64),
      cassete_id              VARCHAR(64),
      wafer_qty               DECIMAL,
      priority_class          VARCHAR(64),
      lot_proc_state          VARCHAR(64),
      last_ope_comp_time      VARCHAR(64),
      eqp_id                  VARCHAR(64),
      ope_no_start_time       VARCHAR(64),
      last_process_start_time VARCHAR(64),
      tech_id                 VARCHAR(64),
      customer_id             VARCHAR(64),
      parent_lot_id           VARCHAR(64),
      trans_state             VARCHAR(64),
      batch_id                VARCHAR(64),
      sub_lot_type            VARCHAR(64),
      batch_name              VARCHAR(64),
      max_batch_size          VARCHAR(64)
    )
    """.format(table_type=table_type)
    duck_db.sql(sql)
    sql = """ 
    create {table_type} table APS_TMP_ETL_RLS_DYB_LOT_RCP_LOAD
                        (
                          lot_id                  VARCHAR(64),
                          prodspec_id             VARCHAR(64),
                          current_ope_no          VARCHAR(64),
                          dyb_mainpd_id           VARCHAR(64),
                          pos_pdid                VARCHAR(64),
                          ope_no                  VARCHAR(64),
                          ope_seq                 VARCHAR(64),
                          return_mainpd_id        VARCHAR(64),
                          return_ope_no           VARCHAR(64),
                          lot_type                VARCHAR(64),
                          cassete_id              VARCHAR(64),
                          wafer_qty               DECIMAL,
                          priority_class          VARCHAR(64),
                          lot_proc_state          VARCHAR(64),
                          last_ope_comp_time      VARCHAR(64),
                          eqp_id                  VARCHAR(64),
                          ope_no_start_time       VARCHAR(64),
                          last_process_start_time VARCHAR(64),
                          tech_id                 VARCHAR(64),
                          customer_id             VARCHAR(64),
                          parent_lot_id           VARCHAR(64),
                          lcrecipe_id             VARCHAR(64),
                          trans_state             VARCHAR(64),
                          batch_id                VARCHAR(64),
                          sub_lot_type            VARCHAR(64),
                        batch_name              VARCHAR(64),
                        m_recipe                VARCHAR(128),
                        physical_recipe_id      VARCHAR(64),
                        recipe_id_c             VARCHAR(64),
                        max_batch_size          VARCHAR(64)
                        )
                        """.format(table_type=table_type)
    duck_db.sql(sql)
    sql = """ 
    create {table_type} table APS_TMP_ETL_RLS_DYB_LOT_TOOL
    (
      lot_id                  VARCHAR(64),
      prodspec_id             VARCHAR(64),
      current_ope_no          VARCHAR(64),
      dyb_mainpd_id           VARCHAR(64),
      pos_pdid                VARCHAR(64),
      ope_no                  VARCHAR(64),
      step_id                 VARCHAR(64),
      return_mainpd_id        VARCHAR(64),
      return_ope_no           VARCHAR(64),
      lot_type                VARCHAR(64),
      cassete_id              VARCHAR(64),
      wafer_qty               DECIMAL,
      priority_class          VARCHAR(64),
      lot_proc_state          VARCHAR(64),
      last_ope_comp_time      VARCHAR(64),
      eqp_id                  VARCHAR(64),
      ope_no_start_time       VARCHAR(64),
      last_process_start_time VARCHAR(64),
      tech_id                 VARCHAR(64),
      customer_id             VARCHAR(64),
      parent_lot_id           VARCHAR(64),
      recipe                  VARCHAR(64),
      eqp_g                   VARCHAR(64),
      eqp_category            VARCHAR(64),
      trans_state             VARCHAR(64),
      batch_id                VARCHAR(64),
      sub_lot_type            VARCHAR(64),
        batch_name              VARCHAR(64),
        m_recipe                VARCHAR(128),
        physical_recipe_id      VARCHAR(64),
        recipe_id_c             VARCHAR(64),
        lcrecipe_id             VARCHAR(64),
        max_batch_size          VARCHAR(64)
    )
    """.format(table_type=table_type)
    duck_db.sql(sql)
    sql = """
    create {table_type} table APS_TMP_ETL_RLS_WIP_FLOW
    (
      prod_id     VARCHAR(64),
      prodspec_id VARCHAR(64),
      plan_id     VARCHAR(64),
      mainpd_id   VARCHAR(64),
      plan_no     VARCHAR(64),
      wip_ope_no  VARCHAR(64),
      ope_no      VARCHAR(64),
      step_id     VARCHAR(64),
      toolg_id    VARCHAR(64),
      prodg_id    VARCHAR(64),
      prodg_tech  VARCHAR(64),
      toolg_type  VARCHAR(64)
    )
    """.format(table_type=table_type)
    duck_db.sql(sql)
    sql = """ 
    create {table_type} table APS_TMP_ETL_RLS_WIP_PO_N1
    (
      prod_id     VARCHAR(64),
      prodspec_id VARCHAR(64),
      plan_id     VARCHAR(64),
      mainpd_id   VARCHAR(64),
      plan_no     VARCHAR(64),
      wip_ope_no  VARCHAR(64),
      ope_no      VARCHAR(64),
      step_id     VARCHAR(64),
      toolg_id    VARCHAR(64),
      prodg_id    VARCHAR(64),
      prodg_tech  VARCHAR(64),
      toolg_type  VARCHAR(64),
      pos_pdid    VARCHAR(64)
    )
    """.format(table_type=table_type)
    duck_db.sql(sql)
    sql = """ 
    create {table_type} table APS_TMP_ETL_RLS_WIP_RCP_LOAD
    (
      prod_id    VARCHAR(64),
      plan_id    VARCHAR(64),
      plan_no    VARCHAR(64),
      ope_no     VARCHAR(64),
      step_id    VARCHAR(64),
      recipe     VARCHAR(64),
      toolg_id   VARCHAR(64),
      prodg_id   VARCHAR(64),
      prodg_tech VARCHAR(64),
      step_type  VARCHAR(64),
      toolg_type VARCHAR(64)
    )
    """.format(table_type=table_type)
    duck_db.sql(sql)
    sql = """  
    create {table_type} table APS_TMP_ETL_RLS_WIP_COMING_DATE
    (
      parent_id          VARCHAR(64),
      lot_id             VARCHAR(64),
      prodspec_id        VARCHAR(64),
      plan_id            VARCHAR(64),
      target_plan_id     VARCHAR(64),
      ope_no             VARCHAR(64),
      ope_seq            VARCHAR(64),
      target_ope_no      VARCHAR(64),
      target_ope_seq     VARCHAR(64),
      target_toolg_id    VARCHAR(64),
      lot_type           VARCHAR(64),
      lot_finished_state VARCHAR(64),
      foup_id            VARCHAR(64),
      wafer_qty          DECIMAL,
      lot_status         VARCHAR(64),
      flow_recipe        VARCHAR(64),
      create_time        VARCHAR(64),
      trans_state        VARCHAR(64),
      batch_id           VARCHAR(64),
      sub_lot_type       VARCHAR(64)
    )
    """.format(table_type=table_type)
    duck_db.sql(sql)
    sql = """ 
    create {table_type} table APS_TMP_ETL_RLS_WIP_COMING_FLOW_DATE
    (
      parent_id       VARCHAR(64),
      lot_id          VARCHAR(64),
      step_id         VARCHAR(64),
      wip_ope_no      VARCHAR(64),
      target_step_id  VARCHAR(64),
      target_ope_no   VARCHAR(64),
      target_toolg_id VARCHAR(64),
      prodspec_id     VARCHAR(64),
      plan_id         VARCHAR(64),
      target_plan_id  VARCHAR(64),
      lot_type        VARCHAR(64),
      foup_id         VARCHAR(64),
      wafer_qty       DECIMAL,
      lot_status      VARCHAR(64),
      flow_recipe     VARCHAR(64),
      trans_state     VARCHAR(64),
      batch_id        VARCHAR(64),
      rn              DECIMAL,
      sub_lot_type    VARCHAR(64)
    )
    """.format(table_type=table_type)
    duck_db.sql(sql)
    sql = """ 
    create {table_type} table APS_TMP_ETL_RLS_EQP_TOOL
    (
      eqp_g            VARCHAR(64),
      eqp_chamber_flag VARCHAR(64),
      eqp_id           VARCHAR(64),
      module           VARCHAR(64),
      EQP_CATEGORY     VARCHAR(64)
    )
    """.format(table_type=table_type)
    duck_db.sql(sql)
    sql = """ 
    create {table_type} table APS_TMP_ETL_RLS_EQPRCP_RESULT
    (
      parent_id          VARCHAR(64),
      lot_id             VARCHAR(64),
      step_id            VARCHAR(64),
      ope_no             VARCHAR(64),
      target_step_id     VARCHAR(64),
      target_ope_no      VARCHAR(64),
      plan_id            VARCHAR(64),
      target_plan_id     VARCHAR(64),
      prod_id            VARCHAR(64),
      pd_id              VARCHAR(64),
      target_toolg_id    VARCHAR(64),
      eqp_id             VARCHAR(64),
      qty                DECIMAL,
      m_recipe           VARCHAR(100),
      physical_recipe_id VARCHAR(60),
      recipe_id_c        VARCHAR(64),
      flow_recipe        VARCHAR(64),
      ppid               VARCHAR(100),
      lot_eqp_status     VARCHAR(64),
      createtime         VARCHAR(64),
      apc_pilot_status   VARCHAR(64),
      eqp_chamber_flag   VARCHAR(64),
      trans_state        VARCHAR(64),
      batch_name         VARCHAR(64),
      cassete_id         VARCHAR(64),
      lcrecipe_id        VARCHAR(64),
      batch_id           VARCHAR(64),
      sub_lot_type       VARCHAR(64),
      module             VARCHAR(64),
      LOT_PROC_STATE     VARCHAR(64),
      max_batch_size     DECIMAL,
      monitor_flag       VARCHAR(1)
    )
    """.format(table_type=table_type)
    duck_db.sql(sql)
    sql = """ 
    create {table_type} table APS_TMP_ETL_RLS_EQPRCP_RESULT1
    (
      lot_id         VARCHAR(64),
      ope_seq        VARCHAR(64),
      ope_no         VARCHAR(64),
      target_ope_seq VARCHAR(64),
      target_ope_no  VARCHAR(64),
      plan_id        VARCHAR(64),
      target_plan_id VARCHAR(64),
      prodspec_id    VARCHAR(64),
      wafer_qty      DECIMAL,
      flow_recipe    VARCHAR(64),
      trans_state    VARCHAR(64),
      batch_name     VARCHAR(64),
      foup_id        VARCHAR(64),
      batch_id       VARCHAR(64),
      pos_pdid       VARCHAR(64),
      sub_lot_type   VARCHAR(64),
      LOT_PROC_STATE     VARCHAR(64),   
      max_batch_size DECIMAL
    )
    """.format(table_type=table_type)
    duck_db.sql(sql)
    sql = """
    create {table_type} table APS_TMP_ETL_RLS_EQPRCP_RESULT2
    (
      lot_id             VARCHAR(64),
      ope_seq            VARCHAR(64),
      ope_no             VARCHAR(64),
      target_ope_seq     VARCHAR(64),
      target_ope_no      VARCHAR(64),
      plan_id            VARCHAR(64),
      target_plan_id     VARCHAR(64),
      prodspec_id        VARCHAR(64),
      pd_id              VARCHAR(64),
      eqp_id             VARCHAR(64),
      wafer_qty          DECIMAL,
      m_recipe           VARCHAR(64),
      physical_recipe_id VARCHAR(64),
      recipe_id_c        VARCHAR(64),
      flow_recipe        VARCHAR(64),
      ppid               VARCHAR(64),
      trans_state        VARCHAR(64),
      batch_name         VARCHAR(64),
      foup_id            VARCHAR(64),
      lcrecipe_id        VARCHAR(64),
      batch_id           VARCHAR(64),
      sub_lot_type       VARCHAR(64),
      LOT_PROC_STATE     VARCHAR(64),
      max_batch_size     DECIMAL
    )
    """.format(table_type=table_type)
    duck_db.sql(sql)
    sql = """ 
    create {table_type} table APS_TMP_ETL_RLS_EQPRCP_RESULT3
    (
      lot_id             VARCHAR(64),
      ope_seq            VARCHAR(64),
      ope_no             VARCHAR(64),
      target_ope_seq     VARCHAR(64),
      target_ope_no      VARCHAR(64),
      plan_id            VARCHAR(64),
      target_plan_id     VARCHAR(64),
      prodspec_id        VARCHAR(64),
      pd_id              VARCHAR(64),
      eqp_g              VARCHAR(64),
      eqp_id             VARCHAR(64),
      wafer_qty          DECIMAL,
      m_recipe           VARCHAR(64),
      physical_recipe_id VARCHAR(64),
      recipe_id_c        VARCHAR(64),
      flow_recipe        VARCHAR(64),
      ppid               VARCHAR(64),
      eqp_chamber_flag   VARCHAR(64),
      trans_state        VARCHAR(64),
      batch_name         VARCHAR(64),
      foup_id            VARCHAR(64),
      lcrecipe_id        VARCHAR(64),
      batch_id           VARCHAR(64),
      sub_lot_type       VARCHAR(64),
      module             VARCHAR(64),
      LOT_PROC_STATE     VARCHAR(64),
      max_batch_size     DECIMAL,
      monitor_flag       VARCHAR(1)
    )
    """.format(table_type=table_type)
    duck_db.sql(sql)
    sql = """ 
    create {table_type} table APS_TMP_ETL_RLS_LOT_TO_EQP
    (
      lot_id VARCHAR(64),
      ope_no VARCHAR(64),
      eqp_id VARCHAR(64)
    )
    """.format(table_type=table_type)
    duck_db.sql(sql)
    sql = """ 
    create {table_type} table APS_TMP_ETL_RLS_EQPRCPRTC_RESULT
    (
      parent_id          VARCHAR(60),
      lot_id             VARCHAR(64),
      step_id            VARCHAR(64),
      ope_no             VARCHAR(64),
      target_step_id     VARCHAR(64),
      target_ope_no      VARCHAR(64),
      plan_id            VARCHAR(64),
      target_plan_id     VARCHAR(64),
      prod_id            VARCHAR(64),
      pd_id              VARCHAR(64),
      target_toolg_id    VARCHAR(64),
      eqp_id             VARCHAR(64),
      qty                DECIMAL,
      m_recipe           VARCHAR(100),
      physical_recipe_id VARCHAR(60),
      recipe_id_c        VARCHAR(64),
      flow_recipe        VARCHAR(64),
      ppid               VARCHAR(100),
      lot_eqp_status     VARCHAR(64),
      createtime         VARCHAR(60),
      reticle_id         VARCHAR(128),
      reticle_recipe     VARCHAR(64),
      chamber_set        VARCHAR(512),
      batch_size_max     DECIMAL,
      batch_size_min     DECIMAL,
      apc_pilot_status   VARCHAR(60),
      eqp_chamber_flag   VARCHAR(60),
      trans_state        VARCHAR(64),
      batch_name         VARCHAR(64),
      cassete_id         VARCHAR(64),
      lcrecipe_id        VARCHAR(64),
      batch_id           VARCHAR(64),
      csfr_plan_id       VARCHAR(64),
      chipbody           VARCHAR(64),
      sub_lot_type       VARCHAR(64),
      module             VARCHAR(64),
      lot_proc_state     VARCHAR(64)
    )
    """.format(table_type=table_type)
    duck_db.sql(sql)
    # sql = """
    # create {table_type} table APS_TMP_ETL_RLS_BATCH_SIZE_SETTING
    # (
    #   eqp_id        VARCHAR(64),
    #   prod_id       VARCHAR(64),
    #   target_ope_no VARCHAR(64),
    #   batch_size    DECIMAL
    # )
    # """
    # duck_db.sql(sql)
    sql = """ 
    create {table_type} table APS_TMP_ETL_RLS_FOUP_RECIPE
    (
      lot_id         VARCHAR(64),
      target_ope_no  VARCHAR(64),
      target_plan_id VARCHAR(64),
      prod_id        VARCHAR(64)
    )
    """.format(table_type=table_type)
    duck_db.sql(sql)
    sql = """ 
    create {table_type} table APS_TMP_ETL_RLS_REWORK_COMING
    (
      lot_mother VARCHAR(64),
      lot_son    VARCHAR(64),
      ope_no     VARCHAR(64),
      eqp_id     VARCHAR(64)
    )
    """.format(table_type=table_type)
    duck_db.sql(sql)
    sql = """ 
    create {table_type} table APS_TMP_ETL_RLS_REWORK_COMING_SELF
    (
      lot_mother VARCHAR(64),
      lot_son    VARCHAR(64),
      ope_no     VARCHAR(64),
      eqp_id     VARCHAR(64)
    )
    """.format(table_type=table_type)
    duck_db.sql(sql)
    sql = """ 
    create {table_type} table APS_TMP_ETL_RLS_SPLIT_INFO
    (
      lot_mother VARCHAR(64),
      lot_son    VARCHAR(64)
    )
    """.format(table_type=table_type)
    duck_db.sql(sql)
    sql = """ 
    create {table_type} table APS_TMP_ETL_RLS_APC_SPLIT_INFO
    (
      parent_id      VARCHAR(60),
      lot_id         VARCHAR(64),
      step_id        VARCHAR(64),
      ope_no         VARCHAR(64),
      target_step_id VARCHAR(64),
      target_ope_no  VARCHAR(64),
      plan_id        VARCHAR(64),
      target_plan_id VARCHAR(64),
      prod_id        VARCHAR(64),
      eqp_id         VARCHAR(64),
      flow_recipe    VARCHAR(60),
      ppid           VARCHAR(60),
      unit_id        VARCHAR(60),
      lot_mother     VARCHAR(60),
      lot_son        VARCHAR(60),
      reticle_id     VARCHAR(60)
    )
    """.format(table_type=table_type)
    duck_db.sql(sql)
    sql = """ 
    create {table_type} table APS_TMP_ETL_RLS_LOTSPECIAL_RESULT
    (
      parent_id  VARCHAR(60),
      lot_id     VARCHAR(64),
      ope_no     VARCHAR(64),
      eqp_id     VARCHAR(64),
      createtime VARCHAR(60)
    )
    """.format(table_type=table_type)
    duck_db.sql(sql)
    sql = """ 
    create {table_type} table APS_TMP_ETL_RLS_CHAMBER_RULE
    (
      line              VARCHAR(60),
      prod_id           VARCHAR(64),
      ope_no            VARCHAR(64),
      eqp_id            VARCHAR(64),
      recipe_id         VARCHAR(128),
      chamber_user_flag VARCHAR(64),
      ch_id             VARCHAR(512)
    )
    """.format(table_type=table_type)
    duck_db.sql(sql)
    sql = """ 
    create {table_type} table APS_TMP_ETL_RLS_WIP_CHAMBER_RULE
    (
      chamber_rule      VARCHAR(64),
      line              VARCHAR(64),
      prodspec_id       VARCHAR(64),
      ope_no            VARCHAR(64),
      eqp_id            VARCHAR(64),
      recipe_id         VARCHAR(64),
      chamber_user_flag VARCHAR(64)
    )
    """.format(table_type=table_type)
    duck_db.sql(sql)
    sql = """ 
    create {table_type} table APS_TMP_ETL_RLS_CSFR_IHBT
    (
      prod_id       VARCHAR(64),
      plan_id       VARCHAR(64),
      ope_no        VARCHAR(64),
      eqp_id        VARCHAR(64),
      pass_flg_prc  VARCHAR(64),
      pass_flg_mfg  VARCHAR(64),
      prc_upt_user  VARCHAR(64),
      sgs_flag      VARCHAR(64),
      ope_seq       VARCHAR(64),
      sgs_group     VARCHAR(64),
      update_time   VARCHAR(64),
      chipbody_prod VARCHAR(64)
    )
    """.format(table_type=table_type)
    duck_db.sql(sql)
    sql = """ 
    create {table_type} table APS_TMP_ETL_RLS_CSFRIHBT_127CASE
    (
      prod_id      VARCHAR(64),
      plan_id      VARCHAR(64),
      ope_no       VARCHAR(64),
      eqp_id       VARCHAR(64),
      pass_flg_prc VARCHAR(64),
      pass_flg_mfg VARCHAR(64),
      prc_upt_user VARCHAR(64),
      sgs_flag     VARCHAR(64),
      ope_seq      VARCHAR(64),
      sgs_group    VARCHAR(64)
    )
    """.format(table_type=table_type)
    duck_db.sql(sql)
    sql = """ 
    create {table_type} table APS_TMP_ETL_RLS_CSFRIHBT_5CASE
    (
      prod_id      VARCHAR(64),
      plan_id      VARCHAR(64),
      ope_no       VARCHAR(64),
      eqp_id       VARCHAR(64),
      pass_flg_prc VARCHAR(64),
      pass_flg_mfg VARCHAR(64),
      prc_upt_user VARCHAR(64),
      sgs_flag     VARCHAR(64),
      ope_seq      VARCHAR(64),
      sgs_group    VARCHAR(64)
    )
    """.format(table_type=table_type)
    duck_db.sql(sql)
    sql = """ 
    create {table_type} table APS_TMP_ETL_RLS_CSFRIHBT_CHIPBODY
    (
      prod_id      VARCHAR(64),
      plan_id      VARCHAR(64),
      ope_no       VARCHAR(64),
      eqp_id       VARCHAR(64),
      chipbody     VARCHAR(64),
      pass_flg_prc VARCHAR(64),
      pass_flg_mfg VARCHAR(64),
      prc_upt_user VARCHAR(64)
    )
    """.format(table_type=table_type)
    duck_db.sql(sql)
    sql = """ 
    create {table_type} table APS_TMP_ETL_RLS_CSFRIHBT_4CASE
    (
      prod_id      VARCHAR(64),
      plan_id      VARCHAR(64),
      ope_no       VARCHAR(64),
      eqp_id       VARCHAR(64),
      pass_flg_prc VARCHAR(64),
      pass_flg_mfg VARCHAR(64),
      prc_upt_user VARCHAR(64),
      sgs_flag     VARCHAR(64),
      ope_seq      VARCHAR(64),
      sgs_group    VARCHAR(64)
    )
    """.format(table_type=table_type)
    duck_db.sql(sql)
    sql = """ 
    create {table_type} table APS_TMP_ETL_RLS_CSFRIHBT_RESULT
    (
      prod_id      VARCHAR(64),
      plan_id      VARCHAR(64),
      ope_no       VARCHAR(64),
      eqp_id       VARCHAR(64),
      pass_flg_prc VARCHAR(64),
      pass_flg_mfg VARCHAR(64),
      prc_upt_user VARCHAR(64)
    )
    """.format(table_type=table_type)
    duck_db.sql(sql)
    sql = """ 
    create {table_type} table APS_TMP_ETL_RLS_CSFRIHBT_3CASE
    (
      prod_id      VARCHAR(64),
      plan_id      VARCHAR(64),
      ope_no       VARCHAR(64),
      eqp_id       VARCHAR(64),
      pass_flg_prc VARCHAR(64),
      pass_flg_mfg VARCHAR(64),
      prc_upt_user VARCHAR(64),
      sgs_flag     VARCHAR(64),
      ope_seq      VARCHAR(64),
      sgs_group    VARCHAR(64)
    )
    """.format(table_type=table_type)
    duck_db.sql(sql)
    sql = """ 
    create {table_type} table APS_TMP_ETL_RLS_CSFRIHBT_5CASE_SOURCE
    (
      prod_id      VARCHAR(64),
      plan_id      VARCHAR(64),
      ope_no       VARCHAR(64),
      eqp_id       VARCHAR(64),
      pass_flg_prc VARCHAR(64),
      sgs_first_op VARCHAR(64),
      prc_upt_user VARCHAR(64),
      pass_flg_mfg VARCHAR(64),
      rn           DECIMAL
    )
    """.format(table_type=table_type)
    duck_db.sql(sql)
    sql = """ 
    create {table_type} table APS_TMP_ETL_RLS_CSFRIHBT_5CASE_INHIBIT
    (
      prod_id      VARCHAR(64),
      plan_id      VARCHAR(64),
      ope_no       VARCHAR(64),
      eqp_id       VARCHAR(64),
      prc_upt_user VARCHAR(64),
      pass_flg_prc VARCHAR(64),
      pass_flg_mfg VARCHAR(64)
    )
    """.format(table_type=table_type)
    duck_db.sql(sql)
    sql = """ 
    create {table_type} table APS_TMP_ETL_RLS_CSFRIHBT_4CASE_INHIB
    (
      prod_id       VARCHAR(64),
      plan_id       VARCHAR(64),
      ope_no        VARCHAR(64),
      eqp_id        VARCHAR(64),
      pass_flg_prc  VARCHAR(64),
      pass_flg_mfg  VARCHAR(64),
      prc_upt_user  VARCHAR(64),
      update_time   VARCHAR(64),
      chipbody_prod VARCHAR(64)
    )
    """.format(table_type=table_type)
    duck_db.sql(sql)
    sql = """ 
    create {table_type} table APS_TMP_ETL_RLS_CSFRIHBT_3CASE_INHIB
    (
      prod_id       VARCHAR(64),
      plan_id       VARCHAR(64),
      rework_ope_no VARCHAR(64),
      eqp_id        VARCHAR(64),
      pass_flg_prc  VARCHAR(64),
      pass_flg_mfg  VARCHAR(64)
    )
    """.format(table_type=table_type)
    duck_db.sql(sql)
    sql = """ 
    create {table_type} table APS_TMP_ETL_RLS_CSFRIHBT_3CASE_NOTREW
    (
      prod_id VARCHAR(64),
      ope_no  VARCHAR(64),
      eqp_id  VARCHAR(64)
    )
    """.format(table_type=table_type)
    duck_db.sql(sql)
    sql = """ 
    create {table_type} table APS_TMP_ETL_RLS_CSFR_PATH
    (
      prod_id      VARCHAR(64),
      plan_id      VARCHAR(64),
      ope_no       VARCHAR(64),
      eqp_id       VARCHAR(64),
      pass_flg_prc VARCHAR(64),
      pass_flg_mfg VARCHAR(64),
      prc_upt_user VARCHAR(64)
    )
    """.format(table_type=table_type)
    duck_db.sql(sql)
    sql = """ 
    create {table_type} table APS_TMP_ETL_RLS_LOT_TO_EQPG_INHIBIT
    (
        toolg_id     VARCHAR(64),
        lot_id       VARCHAR(64),
        ope_no       VARCHAR(64),
        prod_id      VARCHAR(64),
        plan_id      VARCHAR(64),
        pass_flg_prc VARCHAR(64),
        eqp_id       VARCHAR(64),
        active_flag  VARCHAR(64)
    )
    """.format(table_type=table_type)
    duck_db.sql(sql)
    sql = """ 
    create {table_type} table APS_TMP_ETL_RLS_LOT_TO_EQPG_RECIPE
    (
        toolg_id     VARCHAR(64),
        lot_id       VARCHAR(64),
        ope_no       VARCHAR(64),
        prod_id      VARCHAR(64),
        plan_id      VARCHAR(64),
        pass_flg_prc VARCHAR(64),
        flow_recipe  VARCHAR(64),
        eqp_id       VARCHAR(64),
        active_flag  VARCHAR(64)
    )
    """.format(table_type=table_type)
    duck_db.sql(sql)
    # # TODO
    # sql = """
    # create {table_type} table APS_TMP_ETL_RLS_TYPE21_PT
    # (
    #   module_id VARCHAR(64),
    #   toolg_id  VARCHAR(64),
    #   tool_id   VARCHAR(64),
    #   ppid      VARCHAR(64),
    #   pt        DECIMAL
    # )
    # """
    # duck_db.sql(sql)
    # # TODO
    # sql = """
    # create {table_type} table APS_TMP_ETL_RLS_TYPE221_PT
    # (
    #   module_id VARCHAR(64),
    #   toolg_id  VARCHAR(64),
    #   tool_id   VARCHAR(64),
    #   ch_cnt    VARCHAR(64),
    #   pt        DECIMAL
    # )
    # """
    # duck_db.sql(sql)
    # # TODO
    # sql = """ create {table_type} table APS_TMP_ETL_RLS_TYPE22_PT
    #                     (
    #                       module_id VARCHAR(64),
    #                       toolg_id  VARCHAR(64),
    #                       ppid      VARCHAR(64),
    #                       pt        DECIMAL
    #                     )"""
    # duck_db.sql(sql)
    # # TODO
    # sql = """
    # create {table_type} table APS_TMP_ETL_RLS_CHARGE_TIME
    # (
    #   tool_id     VARCHAR(64),
    #   charge_time DECIMAL
    # )
    # """
    # duck_db.sql(sql)

    sql = """ 
    create {table_type} table APS_TMP_ETL_RLS_LOT_SPECIAL
    (
      parent_id          VARCHAR(64),
      lot_id             VARCHAR(64),
      ope_no             VARCHAR(64),
      step_id            VARCHAR(64),
      target_step_id     VARCHAR(64),
      target_ope_no      VARCHAR(64),
      plan_id            VARCHAR(64),
      target_plan_id     VARCHAR(64),
      prod_id            VARCHAR(64),
      pd_id              VARCHAR(64),
      target_toolg_id    VARCHAR(64),
      eqp_id             VARCHAR(64),
      m_recipe           VARCHAR(64),
      physical_recipe_id VARCHAR(64),
      ppid               VARCHAR(64),
      reticle_id         VARCHAR(64),
      flow_recipe        VARCHAR(64),
      set_status6        VARCHAR(64),
      set_status5        VARCHAR(64),
      batch_size_max     DECIMAL,
      batch_size_min     DECIMAL,
      qty                DECIMAL,
      batch_name         VARCHAR(64),
      cassete_id         VARCHAR(64),
      batch_id           VARCHAR(64),
      eqp_chamber_flag   VARCHAR(64),
      sub_lot_type       VARCHAR(64),
      module             VARCHAR(64),
      lot_proc_state     VARCHAR(64),
      trans_state        VARCHAR(64),
      no_ch_set          VARCHAR(64)
    )
    """.format(table_type=table_type)
    duck_db.sql(sql)
    sql = """ 
    create {table_type} table APS_TMP_ETL_RLS_APC_PILOT
    (
      parent_id          VARCHAR(64),
      lot_id             VARCHAR(64),
      ope_no             VARCHAR(64),
      step_id            VARCHAR(64),
      target_step_id     VARCHAR(64),
      target_ope_no      VARCHAR(64),
      plan_id            VARCHAR(64),
      target_plan_id     VARCHAR(64),
      prod_id            VARCHAR(64),
      pd_id              VARCHAR(64),
      target_toolg_id    VARCHAR(64),
      eqp_id             VARCHAR(64),
      m_recipe           VARCHAR(64),
      physical_recipe_id VARCHAR(64),
      ppid               VARCHAR(64),
      reticle_id         VARCHAR(64),
      flow_recipe        VARCHAR(64),
      set_status6        VARCHAR(64),
      set_status7        VARCHAR(64),
      set_status5        VARCHAR(64),
      batch_size_max     DECIMAL,
      batch_size_min     DECIMAL,
      qty                DECIMAL,
      batch_name         VARCHAR(64),
      cassete_id         VARCHAR(64),
      batch_id           VARCHAR(64),
      eqp_chamber_flag   VARCHAR(64),
      sub_lot_type       VARCHAR(64),
      module             VARCHAR(64),
      lot_proc_state     VARCHAR(64),
      set_status20       VARCHAR(64),
      trans_state        VARCHAR(64),
      set_status2        VARCHAR(64)
    )
    """.format(table_type=table_type)
    duck_db.sql(sql)
    sql = """ 
    create {table_type} table APS_TMP_ETL_RLS_APC_PILOT_RCP
    (
      parent_id          VARCHAR(64),
      lot_id             VARCHAR(64),
      ope_no             VARCHAR(64),
      step_id            VARCHAR(64),
      target_step_id     VARCHAR(64),
      target_ope_no      VARCHAR(64),
      plan_id            VARCHAR(64),
      target_plan_id     VARCHAR(64),
      prod_id            VARCHAR(64),
      pd_id              VARCHAR(64),
      target_toolg_id    VARCHAR(64),
      eqp_id             VARCHAR(64),
      m_recipe           VARCHAR(64),
      physical_recipe_id VARCHAR(64),
      ppid               VARCHAR(64),
      reticle_id         VARCHAR(64),
      flow_recipe        VARCHAR(64),
      set_status6        VARCHAR(64),
      set_status7        VARCHAR(64),
      set_status5        VARCHAR(64),
      batch_size_max     DECIMAL,
      batch_size_min     DECIMAL,
      qty                DECIMAL,
      batch_name         VARCHAR(64),
      cassete_id         VARCHAR(64),
      batch_id           VARCHAR(64),
      eqp_chamber_flag   VARCHAR(64),
      sub_lot_type       VARCHAR(64),
      set_status2        CHAR(14),
      module             VARCHAR(64),
      lot_proc_state     VARCHAR(64),
      set_status20       VARCHAR(64),
      trans_state        VARCHAR(64)
    )
    """.format(table_type=table_type)
    duck_db.sql(sql)
    sql = """ 
    create {table_type} table APS_TMP_ETL_RLS
    (
      parent_id            VARCHAR(60),
      lot_id               VARCHAR(60),
      step_id              VARCHAR(60),
      ope_no               VARCHAR(60),
      target_step_id       VARCHAR(60),
      target_ope_no        VARCHAR(60),
      plan_id              VARCHAR(60),
      target_plan_id       VARCHAR(60),
      prod_id              VARCHAR(60),
      pd_id                VARCHAR(60),
      target_toolg_id      VARCHAR(60),
      eqp_id               VARCHAR(60),
      qty                  DECIMAL,
      m_recipe             VARCHAR(100),
      physical_recipe_id   VARCHAR(60),
      photo_flag           VARCHAR(60),
      reticle_id           VARCHAR(60),
      active_rtcl          VARCHAR(60),
      chamber_set          VARCHAR(300),
      set_status           VARCHAR(512),
      batch_size_max       DECIMAL,
      batch_size_min       DECIMAL,
      flow_recipe          VARCHAR(100),
      other_min_batch_size DECIMAL,
      non_wet_batch        VARCHAR(2),
      module               VARCHAR(64)
    )
    """.format(table_type=table_type)
    duck_db.sql(sql)
    sql = """ 
    create {table_type} table APS_TMP_ETL_RLS_EQPG
    (
      parent_id            VARCHAR(60),
      lot_id               VARCHAR(60),
      step_id              VARCHAR(60),
      ope_no               VARCHAR(60),
      target_step_id       VARCHAR(60),
      target_ope_no        VARCHAR(60),
      plan_id              VARCHAR(60),
      target_plan_id       VARCHAR(60),
      prod_id              VARCHAR(60),
      pd_id                VARCHAR(60),
      target_toolg_id      VARCHAR(60),
      eqp_id               VARCHAR(60),
      qty                  DECIMAL,
      m_recipe             VARCHAR(100),
      physical_recipe_id   VARCHAR(60),
      photo_flag           VARCHAR(60),
      reticle_id           VARCHAR(60),
      active_rtcl          VARCHAR(60),
      chamber_set          VARCHAR(300),
      set_status           VARCHAR(512),
      batch_size_max       DECIMAL,
      batch_size_min       DECIMAL,
      flow_recipe          VARCHAR(100),
      other_min_batch_size DECIMAL,
      non_wet_batch        VARCHAR(2),
      module               VARCHAR(64),
      CH_CNT               VARCHAR(64),
      BATCH_EQP_FLAG       VARCHAR(1),
      PARMODE_FLAG         VARCHAR(1),
      CH_COUNT             VARCHAR(64),
      CHAMBER_SET_TWIN     VARCHAR(64),
      PRODG5               VARCHAR(64),
      PRODG3               VARCHAR(64)
    )
    """.format(table_type=table_type)
    duck_db.sql(sql)
    sql = """ 
    create {table_type} table APS_TMP_ETL_RLS_RESULT
    (
      parentid             VARCHAR(60),
      lot_id               VARCHAR(64),
      step_id              VARCHAR(64),
      ope_no               VARCHAR(64),
      target_step_id       VARCHAR(64),
      target_ope_no        VARCHAR(64),
      plan_id              VARCHAR(64),
      target_plan_id       VARCHAR(64),
      toolg_id             VARCHAR(64),
      tool_id              VARCHAR(64),
      recipe               VARCHAR(64),
      ppid                 VARCHAR(64),
      reticle_id           VARCHAR(128),
      ch_set               VARCHAR(512),
      set_status           VARCHAR(512),
      qty                  DECIMAL,
      max_batch_size       DECIMAL,
      min_batch_size       DECIMAL,
      non_process_runtime  DECIMAL,
      process_runtime      DECIMAL,
      update_time          VARCHAR(64),
      partcode             VARCHAR(60),
      other_min_batch_size DECIMAL,
      recipe_setup         VARCHAR(64),
      rls_attr1            VARCHAR(64),
      rls_attr3            VARCHAR(64),
      PER_PROCESS_RUNTIME  VARCHAR(64)
    )
    """.format(table_type=table_type)
    duck_db.sql(sql)
    sql = """ 
    create {table_type} table APS_TMP_ETL_RLS_TR_RESULT
    (
      parent_id            VARCHAR(64),
      lot_id               VARCHAR(64),
      step_id              VARCHAR(64),
      ope_no               VARCHAR(64),
      target_step_id       VARCHAR(64),
      target_ope_no        VARCHAR(64),
      plan_id              VARCHAR(64),
      target_plan_id       VARCHAR(64),
      target_toolg_id      VARCHAR(64),
      eqp_id               VARCHAR(64),
      prod_id              VARCHAR(64),
      flow_recipe          VARCHAR(64),
      ppid                 VARCHAR(64),
      photo_flag           VARCHAR(64),
      reticle_id           VARCHAR(64),
      chamber_set          VARCHAR(512),
      set_status           VARCHAR(512),
      qty                  DECIMAL,
      batch_size_max       DECIMAL,
      batch_size_min       DECIMAL,
      npt                  DECIMAL,
      pt                   DECIMAL,
      other_min_batch_size DECIMAL,
      non_wet_batch        VARCHAR(2),
      module               VARCHAR(64),
      ch_cnt               DECIMAL,
      BATCH_EQP_FLAG       VARCHAR(1),
      PARMODE_FLAG         VARCHAR(1),
      CH_COUNT             VARCHAR(64),
      CHAMBER_SET_TWIN     VARCHAR(64),
      PRODG5               VARCHAR(64),
      PRODG3               VARCHAR(64),
      LAYER                VARCHAR(64),
      M_RECIPE             VARCHAR(64)
    )
    """.format(table_type=table_type)
    duck_db.sql(sql)
    sql = """ 
    create {table_type} table APS_TMP_ETL_RLS_TR_GLOBAL_RESULT
    (
      parent_id            VARCHAR(64),
      lot_id               VARCHAR(64),
      step_id              VARCHAR(64),
      ope_no               VARCHAR(64),
      target_step_id       VARCHAR(64),
      target_ope_no        VARCHAR(64),
      plan_id              VARCHAR(64),
      target_plan_id       VARCHAR(64),
      target_toolg_id      VARCHAR(64),
      eqp_id               VARCHAR(64),
      prod_id              VARCHAR(64),
      flow_recipe          VARCHAR(64),
      ppid                 VARCHAR(64),
      photo_flag           VARCHAR(64),
      reticle_id           VARCHAR(64),
      chamber_set          VARCHAR(512),
      set_status           VARCHAR(512),
      qty                  DECIMAL,
      batch_size_max       DECIMAL,
      batch_size_min       DECIMAL,
      npt                  DECIMAL,
      pt                   DECIMAL,
      other_min_batch_size DECIMAL,
      M_RECIPE             VARCHAR(64),
      PER_PROCESS_RUNTIME  VARCHAR(64)
    )
    """.format(table_type=table_type)
    duck_db.sql(sql)
    sql = """ 
    create {table_type} table APS_TMP_ETL_RLS_TRANS_STATE
    (
      lot_id      VARCHAR(64),
      mainpd_id   VARCHAR(64),
      prodspec_id VARCHAR(64),
      ope_no      VARCHAR(64)
    )
    """.format(table_type=table_type)
    duck_db.sql(sql)
    sql = """ 
    create {table_type} table APS_TMP_ETL_RLS_RTD_QTIME
    (
        parent_id          VARCHAR(64),
        lot_id             VARCHAR(64),
        ope_no             VARCHAR(64),
        step_id            VARCHAR(64),
        target_step_id     VARCHAR(64),
        target_ope_no      VARCHAR(64),
        plan_id            VARCHAR(64),
        target_plan_id     VARCHAR(64),
        prod_id            VARCHAR(64),
        pd_id              VARCHAR(64),
        target_toolg_id    VARCHAR(64),
        eqp_id             VARCHAR(64),
        m_recipe           VARCHAR(64),
        physical_recipe_id VARCHAR(64),
        ppid               VARCHAR(64),
        reticle_id         VARCHAR(64),
        flow_recipe        VARCHAR(64),
        set_status6        VARCHAR(64),
        set_status7        VARCHAR(64),
        set_status5        VARCHAR(64),
        batch_size_max     DECIMAL,
        batch_size_min     DECIMAL,
        qty                DECIMAL,
        batch_name         VARCHAR(64),
        cassete_id         VARCHAR(64),
        batch_id           VARCHAR(64),
        eqp_chamber_flag   VARCHAR(64),
        sub_lot_type       VARCHAR(64),
        module             VARCHAR(64),
        set_status2        VARCHAR(64),
        set_status18       VARCHAR(512),
        is_btath_flag      VARCHAR(4),
        set_status19       VARCHAR(128),
        lot_proc_state     VARCHAR(64),
        set_status20       VARCHAR(64),
        trans_state        VARCHAR(64)
    )
    """.format(table_type=table_type)
    duck_db.sql(sql)

    sql = """ 
       create {table_type} table APS_TMP_ETL_RLS_ANOMALY_EWS_LOT_DATA
       (
            lot_id      VARCHAR(64),
            ope_no      VARCHAR(64),
            eqp_id      VARCHAR(64),
            prodspec_id VARCHAR(64)
       )
       """.format(table_type=table_type)
    duck_db.sql(sql)

    sql = """ 
           create {table_type} table APS_TMP_ETL_RLS_ANOMALY_EWS_LOT
           (
                parent_id            VARCHAR(64),
                lot_id               VARCHAR(64),
                ope_no               VARCHAR(64),
                step_id              VARCHAR(64),
                target_step_id       VARCHAR(64),
                target_ope_no        VARCHAR(64),
                plan_id              VARCHAR(64),
                target_plan_id       VARCHAR(64),
                prod_id              VARCHAR(64),
                pd_id                VARCHAR(64),
                target_toolg_id      VARCHAR(64),
                eqp_id               VARCHAR(64),
                m_recipe             VARCHAR(64),
                physical_recipe_id   VARCHAR(64),
                ppid                 VARCHAR(64),
                reticle_id           VARCHAR(64),
                flow_recipe          VARCHAR(64),
                set_status6          VARCHAR(64),
                set_status7          VARCHAR(64),
                set_status5          VARCHAR(64),
                batch_size_max       VARCHAR(64),
                batch_size_min       VARCHAR(64),
                qty                  VARCHAR(64),
                batch_name           VARCHAR(64),
                cassete_id           VARCHAR(64),
                batch_id             VARCHAR(64),
                eqp_chamber_flag     VARCHAR(64),
                sub_lot_type         VARCHAR(64),
                module               VARCHAR(64),
                set_status2          VARCHAR(64),
                set_status18         VARCHAR(512),
                is_btath_flag        VARCHAR(4),
                ch_set               VARCHAR(512),
                set_status8          VARCHAR(64),
                other_min_batch_size VARCHAR(64),
                set_status19         VARCHAR(128),
                set_status20         VARCHAR(64),
                set_status21         VARCHAR(64),
                set_status22         VARCHAR(64),
                lot_proc_state       VARCHAR(64),
                trans_state          VARCHAR(64)
           )
           """.format(table_type=table_type)
    duck_db.sql(sql)

    sql = """ 
           create {table_type} table APS_TMP_ETL_RLS_MEASURE_LOT
           (
                parent_id          VARCHAR(64),
                lot_id             VARCHAR(64),
                prodspec_id        VARCHAR(64),
                plan_id            VARCHAR(64),
                target_plan_id     VARCHAR(64),
                ope_no             VARCHAR(64),
                ope_seq            VARCHAR(64),
                target_ope_no      VARCHAR(64),
                target_ope_seq     VARCHAR(64),
                target_toolg_id    VARCHAR(64),
                lot_type           VARCHAR(64),
                lot_finished_state VARCHAR(64),
                foup_id            VARCHAR(64),
                wafer_qty          VARCHAR(64),
                lot_status         VARCHAR(64),
                flow_recipe        VARCHAR(64),
                create_time        VARCHAR(64),
                trans_state        VARCHAR(64),
                batch_id           VARCHAR(64),
                sub_lot_type       VARCHAR(64)
           )
           """.format(table_type=table_type)
    duck_db.sql(sql)

    sql = """ 
       create {table_type} table APS_TMP_ETL_RLS_APC_INHIBIT_LOT_GLOBAL
       (
            prod_id VARCHAR(64),
            ope_no  VARCHAR(64),
            eqp_id  VARCHAR(64)
       )
               """.format(table_type=table_type)
    duck_db.sql(sql)

    sql = """ 
         create {table_type} table APS_TMP_ETL_RLS_DYB_RCP_LOAD
         (
            eqp_id             VARCHAR(64),
            lot_id             VARCHAR(64),
            m_recipe           VARCHAR(128),
            pf_pd_id           VARCHAR(64),
            prodspec_id        VARCHAR(64),
            lcrecipe_id        VARCHAR(64),
            ope_no             VARCHAR(64),
            physical_recipe_id VARCHAR(64),
            recipe_id_c        VARCHAR(64)
         )
                 """.format(table_type=table_type)
    duck_db.sql(sql)

    sql = """ 
         create {table_type} table APS_TMP_ETL_RLS_MORE_RETICLE
         (
            prod_id VARCHAR(64),
            ope_no  VARCHAR(64),
            rtcl_id VARCHAR(128)
         )
                 """.format(table_type=table_type)
    duck_db.sql(sql)

    # TODO
    sql = """ 
           create {table_type} table APS_TMP_ETL_RLS_PH_RTD_QTIME
           (
                parent_id          VARCHAR(64),
                lot_id             VARCHAR(64),
                ope_no             VARCHAR(64),
                step_id            VARCHAR(64),
                target_step_id     VARCHAR(64),
                target_ope_no      VARCHAR(64),
                plan_id            VARCHAR(64),
                target_plan_id     VARCHAR(64),
                prod_id            VARCHAR(64),
                pd_id              VARCHAR(64),
                target_toolg_id    VARCHAR(64),
                eqp_id             VARCHAR(64),
                m_recipe           VARCHAR(64),
                physical_recipe_id VARCHAR(64),
                ppid               VARCHAR(64),
                reticle_id         VARCHAR(64),
                flow_recipe        VARCHAR(64),
                set_status6        VARCHAR(64),
                set_status7        VARCHAR(64),
                set_status5        VARCHAR(64),
                batch_size_max     decimal,
                batch_size_min     decimal,
                qty                decimal,
                batch_name         VARCHAR(64),
                cassete_id         VARCHAR(64),
                batch_id           VARCHAR(64),
                eqp_chamber_flag   VARCHAR(64),
                sub_lot_type       VARCHAR(64),
                module             VARCHAR(64),
                set_status2        VARCHAR(64),
                set_status18       VARCHAR(512),
                is_btath_flag      VARCHAR(4),
                set_status19       VARCHAR(128),
                set_status20       VARCHAR(64),
                lot_proc_state     VARCHAR(64),
                trans_state        VARCHAR(64),
                OTHER_MIN_BATCH_SIZE DECIMAL,
                CH_SET VARCHAR(512),
                SET_STATUS8 VARCHAR(64),
                set_status21       VARCHAR(64),
                set_status22       VARCHAR(64),
                CHIP_PLAN_ID       VARCHAR(64),
                SET_STATUS1        VARCHAR(64),
                SET_STATUS12      VARCHAR(64),
                SET_STATUS23      VARCHAR(64)
           )
                   """.format(table_type=table_type)
    duck_db.sql(sql)

    sql = """ 
           create {table_type} table APS_TMP_ETL_RLS_CHAMBER_RULE_RESULT
           (
                parent_id            VARCHAR(64),
                lot_id               VARCHAR(64),
                ope_no               VARCHAR(64),
                step_id              VARCHAR(64),
                target_step_id       VARCHAR(64),
                target_ope_no        VARCHAR(64),
                plan_id              VARCHAR(64),
                target_plan_id       VARCHAR(64),
                prod_id              VARCHAR(64),
                pd_id                VARCHAR(64),
                target_toolg_id      VARCHAR(64),
                eqp_id               VARCHAR(64),
                m_recipe             VARCHAR(64),
                physical_recipe_id   VARCHAR(64),
                ppid                 VARCHAR(64),
                reticle_id           VARCHAR(64),
                flow_recipe          VARCHAR(64),
                set_status6          VARCHAR(64),
                set_status7          VARCHAR(64),
                set_status5          VARCHAR(64),
                batch_size_max       decimal,
                batch_size_min       decimal,
                qty                  decimal,
                batch_name           VARCHAR(64),
                cassete_id           VARCHAR(64),
                batch_id             VARCHAR(64),
                eqp_chamber_flag     VARCHAR(64),
                sub_lot_type         VARCHAR(64),
                module               VARCHAR(64),
                set_status2          VARCHAR(64),
                set_status18         VARCHAR(512),
                is_btath_flag        VARCHAR(4),
                ch_set               VARCHAR(512),
                set_status8          VARCHAR(64),
                other_min_batch_size decimal,
                set_status19         VARCHAR(128),
                set_status20         VARCHAR(64),
                lot_proc_state       VARCHAR(64),
                trans_state          VARCHAR(64)
           )
                   """.format(table_type=table_type)
    duck_db.sql(sql)

    sql = """ 
           create {table_type} table APS_TMP_ETL_RLS_ONLY_EQP_CHAMBER_RULE
           (
                eqp_id VARCHAR(64)
           )
                   """.format(table_type=table_type)
    duck_db.sql(sql)

    sql = """ 
              create {table_type} table APS_TMP_ETL_RLS_EQP_CHAMBER_RULE
              (
                    eqp_id VARCHAR(64),
                    ch_id  VARCHAR(512)
              )
                      """.format(table_type=table_type)
    duck_db.sql(sql)

    sql = """ 
           create {table_type} table APS_TMP_ETL_RLS_MFG_CHAMBER_RULE
           (
                line      VARCHAR(32),
                prod_id   VARCHAR(64),
                plan_id   VARCHAR(64),
                ope_no    VARCHAR(64),
                eqp_id    VARCHAR(64),
                recipe_id VARCHAR(128),
                act_flg   VARCHAR(2)
           )
                       """.format(table_type=table_type)
    duck_db.sql(sql)

    sql = """ 
          create {table_type} table APS_TMP_ETL_RLS_SETTING_TANK_RULE
          (
            eqp_id VARCHAR(64),
            ch_set VARCHAR(512)
          )
                          """.format(table_type=table_type)
    duck_db.sql(sql)

    sql = """ 
         create {table_type} table APS_TMP_ETL_RLS_TANK_RULE
         (
            eqp_id    VARCHAR(64),
            char_type VARCHAR(64),
            recipe    VARCHAR(256),
            tank_name VARCHAR(128),
            ch_set    VARCHAR(512)
         )
                             """.format(table_type=table_type)
    duck_db.sql(sql)

    sql = """ 
       create {table_type} table APS_TMP_ETL_RLS_NPW_RECIPE_GROUP
       (
              tool_id   VARCHAR(64),
              recipe_id VARCHAR(64),
              group_id  VARCHAR(64)
       )
       """.format(table_type=table_type)
    duck_db.sql(sql)

    sql = """ 
           create {table_type} table APS_TMP_ETL_RLS_RTD_IMP_SOURCE
           (
                mainpd_id VARCHAR(64),
                ope_no    VARCHAR(64),
                source    VARCHAR(64)
           )
                               """.format(table_type=table_type)
    duck_db.sql(sql)

    sql = """ 
           create {table_type} table APS_TMP_ETL_RLS_WIP_MEASURE_COMING_DATE
           (
                parent_id          VARCHAR(64),
                lot_id             VARCHAR(64),
                prodspec_id        VARCHAR(64),
                plan_id            VARCHAR(64),
                target_plan_id     VARCHAR(64),
                ope_no             VARCHAR(64),
                ope_seq            VARCHAR(64),
                target_ope_no      VARCHAR(64),
                target_ope_seq     VARCHAR(64),
                target_toolg_id    VARCHAR(64),
                lot_type           VARCHAR(64),
                lot_finished_state VARCHAR(64),
                foup_id            VARCHAR(64),
                wafer_qty          decimal,
                lot_status         VARCHAR(64),
                flow_recipe        VARCHAR(64),
                create_time        VARCHAR(64),
                trans_state        VARCHAR(64),
                batch_id           VARCHAR(64),
                sub_lot_type       VARCHAR(64)
           )
                               """.format(table_type=table_type)
    duck_db.sql(sql)

    sql = """ 
              create {table_type} table APS_TMP_ETL_RLS_MEASURE_COMING_PO_N1
              (
                parent_id          VARCHAR(64),
                lot_id             VARCHAR(64),
                prodspec_id        VARCHAR(64),
                plan_id            VARCHAR(64),
                target_plan_id     VARCHAR(64),
                ope_no             VARCHAR(64),
                ope_seq            VARCHAR(64),
                target_ope_no      VARCHAR(64),
                target_ope_seq     VARCHAR(64),
                target_toolg_id    VARCHAR(64),
                lot_type           VARCHAR(64),
                lot_finished_state VARCHAR(64),
                foup_id            VARCHAR(64),
                wafer_qty          decimal,
                lot_status         VARCHAR(64),
                flow_recipe        VARCHAR(64),
                create_time        VARCHAR(64),
                trans_state        VARCHAR(64),
                batch_id           VARCHAR(64),
                sub_lot_type       VARCHAR(64),
                pos_pdid           VARCHAR(64),
                mpd_id             VARCHAR(64)
              )
                                  """.format(table_type=table_type)
    duck_db.sql(sql)

    sql = """ 
                  create {table_type} table APS_TMP_ETL_RLS_MEASURE_COMING_RECIPE
                  (
                     parent_id          VARCHAR(64),
                    lot_id             VARCHAR(64),
                    prodspec_id        VARCHAR(64),
                    plan_id            VARCHAR(64),
                    target_plan_id     VARCHAR(64),
                    ope_no             VARCHAR(64),
                    ope_seq            VARCHAR(64),
                    target_ope_no      VARCHAR(64),
                    target_ope_seq     VARCHAR(64),
                    target_toolg_id    VARCHAR(64),
                    lot_type           VARCHAR(64),
                    lot_finished_state VARCHAR(64),
                    foup_id            VARCHAR(64),
                    wafer_qty          decimal,
                    lot_status         VARCHAR(64),
                    flow_recipe        VARCHAR(64),
                    create_time        VARCHAR(64),
                    trans_state        VARCHAR(64),
                    batch_id           VARCHAR(64),
                    sub_lot_type       VARCHAR(64),
                    mpd_id             VARCHAR(64),
                    eqp_id             VARCHAR(64)
                  )
                                      """.format(table_type=table_type)
    duck_db.sql(sql)

    sql = """ 
                   create {table_type} table APS_TMP_ETL_RLS_MEASURE_COMING_CSF
                   (
                        parent_id          VARCHAR(64),
                        lot_id             VARCHAR(64),
                        prodspec_id        VARCHAR(64),
                        plan_id            VARCHAR(64),
                        target_plan_id     VARCHAR(64),
                        ope_no             VARCHAR(64),
                        ope_seq            VARCHAR(64),
                        target_ope_no      VARCHAR(64),
                        target_ope_seq     VARCHAR(64),
                        target_toolg_id    VARCHAR(64),
                        lot_type           VARCHAR(64),
                        lot_finished_state VARCHAR(64),
                        foup_id            VARCHAR(64),
                        wafer_qty          decimal,
                        lot_status         VARCHAR(64),
                        flow_recipe        VARCHAR(64),
                        create_time        VARCHAR(64),
                        trans_state        VARCHAR(64),
                        batch_id           VARCHAR(64),
                        sub_lot_type       VARCHAR(64),
                        mpd_id             VARCHAR(64),
                        user_flag          VARCHAR(1),
                        recipe_flag        VARCHAR(1),
                        eqp_id             VARCHAR(64)
                   )
                                       """.format(table_type=table_type)
    duck_db.sql(sql)

    sql = """ 
                   create {table_type} table APS_TMP_ETL_RLS_MEASURE_COMING_TOOL
                   (
                        parent_id          VARCHAR(64),
                        lot_id             VARCHAR(64),
                        prodspec_id        VARCHAR(64),
                        plan_id            VARCHAR(64),
                        target_plan_id     VARCHAR(64),
                        ope_no             VARCHAR(64),
                        ope_seq            VARCHAR(64),
                        target_ope_no      VARCHAR(64),
                        target_ope_seq     VARCHAR(64),
                        target_toolg_id    VARCHAR(64),
                        lot_type           VARCHAR(64),
                        lot_finished_state VARCHAR(64),
                        foup_id            VARCHAR(64),
                        wafer_qty          decimal,
                        lot_status         VARCHAR(64),
                        flow_recipe        VARCHAR(64),
                        create_time        VARCHAR(64),
                        trans_state        VARCHAR(64),
                        batch_id           VARCHAR(64),
                        sub_lot_type       VARCHAR(64),
                        mpd_id             VARCHAR(64),
                        user_flag          VARCHAR(1),
                        recipe_flag        VARCHAR(1),
                        eqp_id             VARCHAR(64),
                        tool_flag          VARCHAR(1),
                        inhibit_flag       VARCHAR(1)
                   )
                                       """.format(table_type=table_type)
    duck_db.sql(sql)

    sql = """ 
     create {table_type} table APS_TMP_ETL_RLS_MES_FRLRCP
     (
        lot_id           VARCHAR(64),
        target_ope_no    VARCHAR(64),
        target_plan_id   VARCHAR(64),
        prod_id          VARCHAR(64),
        eqp_id           VARCHAR(64),
        flow_recipe      VARCHAR(64),
        mntr_prodspec_id VARCHAR(64)
     )
     """.format(table_type=table_type)
    duck_db.sql(sql)

    sql = """ 
     create {table_type} table APS_TMP_ETL_RLS_MES_FRLRCP_CSF
     (
        lot_id           VARCHAR(64),
        target_ope_no    VARCHAR(64),
        target_plan_id   VARCHAR(64),
        prod_id          VARCHAR(64),
        eqp_id           VARCHAR(64),
        flow_recipe      VARCHAR(64),
        mntr_prodspec_id VARCHAR(64),
        set_status23     VARCHAR(64)
     )
     """.format(table_type=table_type)
    duck_db.sql(sql)

    sql = """ 
      create {table_type} table APS_TMP_ETL_RLS_MONITOR_RECIPE
      (
        parent_id            VARCHAR(64),
        lot_id               VARCHAR(64),
        ope_no               VARCHAR(64),
        step_id              VARCHAR(64),
        target_step_id       VARCHAR(64),
        target_ope_no        VARCHAR(64),
        plan_id              VARCHAR(64),
        target_plan_id       VARCHAR(64),
        prod_id              VARCHAR(64),
        pd_id                VARCHAR(64),
        target_toolg_id      VARCHAR(64),
        eqp_id               VARCHAR(64),
        m_recipe             VARCHAR(64),
        physical_recipe_id   VARCHAR(64),
        ppid                 VARCHAR(64),
        reticle_id           VARCHAR(64),
        flow_recipe          VARCHAR(64),
        set_status6          VARCHAR(64),
        set_status7          VARCHAR(64),
        set_status5          VARCHAR(64),
        batch_size_max       VARCHAR(64),
        batch_size_min       VARCHAR(64),
        qty                  VARCHAR(64),
        batch_name           VARCHAR(64),
        cassete_id           VARCHAR(64),
        batch_id             VARCHAR(64),
        eqp_chamber_flag     VARCHAR(64),
        sub_lot_type         VARCHAR(64),
        module               VARCHAR(64),
        set_status2          VARCHAR(64),
        set_status18         VARCHAR(512),
        is_btath_flag        VARCHAR(4),
        ch_set               VARCHAR(512),
        set_status8          VARCHAR(64),
        other_min_batch_size VARCHAR(64),
        set_status19         VARCHAR(128),
        set_status20         VARCHAR(64),
        set_status21         VARCHAR(64),
        set_status22         VARCHAR(64),
        lot_proc_state       VARCHAR(64),
        set_status23         VARCHAR(64),
        trans_state          VARCHAR(64),
        CHIP_PLAN_ID         VARCHAR(64),
        set_status1          VARCHAR(64),
        set_status12         VARCHAR(64)
      )
      """.format(table_type=table_type)
    duck_db.sql(sql)

    sql = """ 
        create {table_type} table APS_TMP_ETL_RLS_EQPRCP_INHIBIT1
        (
            product_id   VARCHAR(64),
            eqp_id       VARCHAR(64),
            recipe_id    VARCHAR(64),
            reason_code  VARCHAR(64),
            sub_lot_type VARCHAR(64)  
        )
        """.format(table_type=table_type)
    duck_db.sql(sql)
    sql = """ 
        create {table_type} table APS_TMP_ETL_RLS_EQPRCP_INHIBIT2
        (
            product_id   VARCHAR(64),
            eqp_id       VARCHAR(64),
            recipe_id    VARCHAR(64),
            reason_code  VARCHAR(64),
            sub_lot_type VARCHAR(64)
        )
        """.format(table_type=table_type)
    duck_db.sql(sql)
    sql = """ 
        create {table_type} table APS_TMP_ETL_RLS_EQPRCP_INHIBIT3
        (
            product_id   VARCHAR(64),
            eqp_id       VARCHAR(64),
            recipe_id    VARCHAR(64),
            reason_code  VARCHAR(64),
            sub_lot_type VARCHAR(64)
        )
        """.format(table_type=table_type)
    duck_db.sql(sql)
    sql = """ 
        create {table_type} table APS_TMP_ETL_RLS_EQPRCP_INHIBIT4
        (
            product_id   VARCHAR(64),
            eqp_id       VARCHAR(64),
            recipe_id    VARCHAR(64),
            reason_code  VARCHAR(64),
            sub_lot_type VARCHAR(64)
        )
        """.format(table_type=table_type)
    duck_db.sql(sql)
    sql = """ 
          create {table_type} table APS_TMP_ETL_RLS_EQPRCP_INHIBIT5
          (
              product_id   VARCHAR(64),
              eqp_id       VARCHAR(64),
              recipe_id    VARCHAR(64),
              reason_code  VARCHAR(64),
              sub_lot_type VARCHAR(64)
          )
          """.format(table_type=table_type)
    duck_db.sql(sql)
    sql = """ 
          create {table_type} table APS_TMP_ETL_RLS_EQPRCP_INHIBIT6
          (
              product_id   VARCHAR(64),
              eqp_id       VARCHAR(64),
              recipe_id    VARCHAR(64),
              reason_code  VARCHAR(64),
              sub_lot_type VARCHAR(64)
          )
          """.format(table_type=table_type)
    duck_db.sql(sql)
    sql = """ 
           create {table_type} table APS_TMP_ETL_RLS_EQPRCP_INHIBIT7
           (
               product_id   VARCHAR(64),
               eqp_id       VARCHAR(64),
               recipe_id    VARCHAR(64),
               reason_code  VARCHAR(64),
               sub_lot_type VARCHAR(64)
           )
           """.format(table_type=table_type)
    duck_db.sql(sql)

    sql = """ 
       create {table_type} table APS_TMP_ETL_RLS_RDS_RECIPE_INFO
       (
            tool_id          VARCHAR(64),
            runtimegroupname VARCHAR(128),
            ppid             VARCHAR(64)
       )
           """.format(table_type=table_type)
    duck_db.sql(sql)

    sql = """ 
       create {table_type} table APS_TMP_ETL_RLS_QTIME_GRADE
       (
            PROD_ID VARCHAR(64),  
            PLAN_ID VARCHAR(64), 
            FROM_OPE_NO VARCHAR(64), 
            TO_OPE_NO VARCHAR(64)
       )
           """.format(table_type=table_type)
    duck_db.sql(sql)

    sql = """ 
       create {table_type} table APS_TMP_ETL_RLS_QTIME_GRADE_FLOW
       (
            plan_id         VARCHAR(64),
            prod_id         VARCHAR(64),
            trigger_ope_no  VARCHAR(64),
            target_ope_no   VARCHAR(64),
            trigger_step_id VARCHAR(64),
            target_step_id  VARCHAR(64)
       )
           """.format(table_type=table_type)
    duck_db.sql(sql)

    sql = """ 
       create {table_type} table APS_TMP_ETL_RLS_BRACH
       (
              parent_id          VARCHAR(64),
              lot_id             VARCHAR(64),
              prodspec_id        VARCHAR(64),
              plan_id            VARCHAR(64),
              target_plan_id     VARCHAR(64),
              ope_no             VARCHAR(64),
              ope_seq            VARCHAR(64),
              target_ope_no      VARCHAR(64),
              target_ope_seq     VARCHAR(64),
              target_toolg_id    VARCHAR(64),
              lot_type           VARCHAR(64),
              lot_finished_state VARCHAR(64),
              foup_id            VARCHAR(64),
              wafer_qty          VARCHAR(64),
              lot_status         VARCHAR(64),
              flow_recipe        VARCHAR(64),
              create_time        VARCHAR(64),
              trans_state        VARCHAR(64),
              batch_id           VARCHAR(64),
              sub_lot_type       VARCHAR(64)
       )
           """.format(table_type=table_type)
    duck_db.sql(sql)

    sql = """ 
       create {table_type} table APS_TMP_ETL_RLS_ANNOTATION_DATA
       (
              lot_id          VARCHAR(64),
              mainpd_id       VARCHAR(64),
              ope_no          VARCHAR(64),
              annotation_flag VARCHAR(1),
              prodspec_id     VARCHAR(64)
       )
           """.format(table_type=table_type)
    duck_db.sql(sql)

    sql = """ 
       create {table_type} table APS_TMP_ETL_RLS_ANNOTATION_FLOW
       (
              lot_id          VARCHAR(64),
              mainpd_id       VARCHAR(64),
              ope_no          VARCHAR(64),
              annotation_flag VARCHAR(1),
              prodspec_id     VARCHAR(64),
              step_id         VARCHAR(64)
       )
           """.format(table_type=table_type)
    duck_db.sql(sql)

    sql = """ 
       create {table_type} table APS_TMP_ETL_RLS_ANNOTATION_MAX_STEP
       (
              target_plan_id VARCHAR(64),
              prod_id        VARCHAR(64),
              lot_id         VARCHAR(64),
              max_step_id    VARCHAR(64),
              min_step_id    VARCHAR(64)
       )
           """.format(table_type=table_type)
    duck_db.sql(sql)

    sql = """ 
      create {table_type} table APS_TMP_ETL_RLS_ANNOTATION_RESULT
      (
              lot_id        VARCHAR(64),
              prod_id       VARCHAR(64),
              plan_id       VARCHAR(64),
              step_id       VARCHAR(64),
              entry_step_id VARCHAR(64),
              retn_step_id  VARCHAR(64),
              main_plan_id  VARCHAR(64),
              ope_no        VARCHAR(64),
              toolg_id      VARCHAR(64),
              toolg_type    VARCHAR(64)
      )
          """.format(table_type=table_type)
    duck_db.sql(sql)

    sql = """ 
      create {table_type} table APS_TMP_ETL_RLS_ANNOTATION_ALL
      (
              parent_id          VARCHAR(64),
              lot_id             VARCHAR(64),
              prodspec_id        VARCHAR(64),
              plan_id            VARCHAR(64),
              target_plan_id     VARCHAR(64),
              ope_no             VARCHAR(64),
              ope_seq            VARCHAR(64),
              target_ope_no      VARCHAR(64),
              target_ope_seq     VARCHAR(64),
              target_toolg_id    VARCHAR(64),
              lot_type           VARCHAR(64),
              lot_finished_state VARCHAR(64),
              foup_id            VARCHAR(64),
              wafer_qty          VARCHAR(64),
              lot_status         VARCHAR(64),
              flow_recipe        VARCHAR(64),
              create_time        VARCHAR(64),
              trans_state        VARCHAR(64),
              batch_id           VARCHAR(64),
              sub_lot_type       VARCHAR(64),
              toolg_type         VARCHAR(64),
              entry_step_id      VARCHAR(64),
              retn_step_id       VARCHAR(64)
      )
          """.format(table_type=table_type)
    duck_db.sql(sql)

    sql = """ 
      create {table_type} table APS_TMP_ETL_RLS_PATH_SETTING
      (
            use_flag_type VARCHAR(64),
            rs_hold_time  VARCHAR(64),
            tool_id       VARCHAR(64),
            toolg_id      VARCHAR(64)
      )
          """.format(table_type=table_type)
    duck_db.sql(sql)

    sql = """ 
          create {table_type} table APS_TMP_ETL_RLS_APC_PILOT_RCP1
          (
               PROD_ID VARCHAR(64),
               M_RECIPE VARCHAR(64),
               EQP_ID VARCHAR(64),
               SUB_LOT_TYPE VARCHAR(64),
               SET_STATUS2 VARCHAR(64)
          )
              """.format(table_type=table_type)
    duck_db.sql(sql)

    sql = """ 
          create {table_type} table APS_TMP_ETL_RLS_MFG_CHAMBER_RULE1
          (
                M_RECIPE VARCHAR(64), 
                TARGET_OPE_NO VARCHAR(64), 
                PROD_ID VARCHAR(64), 
                TARGET_PLAN_ID VARCHAR(64),
                EQP_ID VARCHAR(64),
                PPID    VARCHAR(64),
                EQP_CHAMBER_FLAG VARCHAR(64) 
          )
          """.format(table_type=table_type)
    duck_db.sql(sql)

    sql = """ 
              create {table_type} table APS_TMP_ETL_RLS_MFG_CHAMBER_RULE2
              (
                    M_RECIPE VARCHAR(64), 
                    TARGET_OPE_NO VARCHAR(64), 
                    PROD_ID VARCHAR(64), 
                    TARGET_PLAN_ID VARCHAR(64),
                    EQP_ID VARCHAR(64),
                    ACT_FLG VARCHAR(1)
              )
              """.format(table_type=table_type)
    duck_db.sql(sql)

    sql = """ 
      create {table_type} table APS_TMP_ETL_RLS_CSFR_PATH_RECIPE
      (
            PROD_ID VARCHAR(64),
            CHIP_PLAN_ID VARCHAR(64),
            TARGET_OPE_NO VARCHAR(64),
            EQP_ID  VARCHAR(64) 
      )
      """.format(table_type=table_type)
    duck_db.sql(sql)

    sql = """ 
     create {table_type} table APS_TMP_ETL_RLS_CHAMBER_RULE_DATA
     (
           PROD_ID VARCHAR(64),
           M_RECIPE VARCHAR(64),
           TARGET_OPE_NO VARCHAR(64),
           EQP_ID VARCHAR(64),
           CH_ID VARCHAR(64),
           CHAMBER_USER_FLAG VARCHAR(1) 
     )
     """.format(table_type=table_type)
    duck_db.sql(sql)

    sql = """ 
         create {table_type} table APS_TMP_ETL_RLS_BEFORE
         (
                PARENT_ID VARCHAR(64),
                LOT_ID VARCHAR(64),
                OPE_NO VARCHAR(64),
                STEP_ID VARCHAR(64),
                TARGET_STEP_ID VARCHAR(64),                                                                                                                                                                         
                TARGET_OPE_NO VARCHAR(64),
                PLAN_ID VARCHAR(64), 
                TARGET_PLAN_ID VARCHAR(64),
                PROD_ID VARCHAR(64),                                                                                                                                                                                
                PD_ID VARCHAR(64),
                TARGET_TOOLG_ID VARCHAR(64),
                EQP_ID VARCHAR(64),
                M_RECIPE VARCHAR(64),                                                                                                                                                                                    
                PHYSICAL_RECIPE_ID VARCHAR(64),
                PPID VARCHAR(64),
                PHOTOFLAG VARCHAR(1),                                                                                                                                                                              
                RETICLE_ID VARCHAR(64),
                FLOW_RECIPE VARCHAR(64), 
                CHAMBER_SET VARCHAR(512),
                SET_STATUS1 VARCHAR(64),
                SET_STATUS2 VARCHAR(64),
                SET_STATUS4 VARCHAR(64),
                SET_STATUS6 VARCHAR(64),
                SET_STATUS7 VARCHAR(64),
                SET_STATUS8 VARCHAR(64),
                SET_STATUS9 VARCHAR(64),
                SET_STATUS10 VARCHAR(64),
                SET_STATUS11 VARCHAR(64),
                SET_STATUS12 VARCHAR(64), 
                SET_STATUS13 VARCHAR(64),
                SET_STATUS14 VARCHAR(64),
                SET_STATUS15 VARCHAR(64),
                SET_STATUS16 VARCHAR(64),
                SET_STATUS17 VARCHAR(64),
                SET_STATUS18 VARCHAR(64),
                SET_STATUS19 VARCHAR(64),
                SET_STATUS20 VARCHAR(64),
                SET_STATUS21 VARCHAR(64),
                SET_STATUS22 VARCHAR(64),
                SET_STATUS23 VARCHAR(64),
                SET_STATUS5 VARCHAR(64),
                BATCH_SIZE_MAX DECIMAL,
                BATCH_SIZE_MIN DECIMAL,
                QTY DECIMAL,                                                                                                                                                                     
                OTHER_MIN_BATCH_SIZE DECIMAL,
                NON_WET_BATCH VARCHAR(64),
                MODULE  VARCHAR(64)
         )
         """.format(table_type=table_type)
    duck_db.sql(sql)

    sql = """ 
        create {table_type} table APS_TMP_ETL_RLS_MES_FRLRCP_CSF1
        (
              LOT_ID VARCHAR(64),
              PROD_ID VARCHAR(64), 
              TARGET_PLAN_ID VARCHAR(64),
              TARGET_OPE_NO VARCHAR(64),
              EQP_ID VARCHAR(64),
              SET_STATUS23 VARCHAR(64)
        )
        """.format(table_type=table_type)
    duck_db.sql(sql)

    sql = """
          create {table_type} table APS_TMP_ETL_RLS_MASK_GROUP_MAPPLING
          (
              OPE_NO        VARCHAR(64),
              PRODSPEC_ID   VARCHAR(64)
          )
          """.format(table_type=table_type)
    duck_db.sql(sql)

    sql = """
          create {table_type} table APS_TMP_ETL_RLS_RTD_PREFER_TOOL
          (
              OPE_NO        VARCHAR(64),
              PRODUCT_ID   VARCHAR(64)
          )
          """.format(table_type=table_type)
    duck_db.sql(sql)

    sql = """
      create {table_type} table APS_TMP_ETL_RLS_APC_SPLIT_INFO1
      (
            LOT_ID VARCHAR(64), 
            TARGET_OPE_NO VARCHAR(64)

      )
      """.format(table_type=table_type)
    duck_db.sql(sql)

    sql = """
          create {table_type} table APS_TMP_ETL_RLS_QTIME_GRADE_FLOW1
          (
                PLAN_ID VARCHAR(64), 
                PROD_ID VARCHAR(64),
                LOT_ID VARCHAR(64), 
                TARGET_STEP_ID VARCHAR(64)

          )
          """.format(table_type=table_type)
    duck_db.sql(sql)

    sql = """
          create {table_type} table APS_TMP_ETL_RLS_RTD_RECIPEGROUP
          (
                TOOL_ID VARCHAR(64),
                PPID VARCHAR(64),
                RTD_GROUPNAME VARCHAR(64)
          )
          """.format(table_type=table_type)
    duck_db.sql(sql)

    sql = """
          create {table_type} table APS_TMP_ETL_RLS_MASK_GROUP_MAPPLING1
          (
                OPE_NO VARCHAR(64),
                PRODSPEC_ID VARCHAR(64)
          )
          """.format(table_type=table_type)
    duck_db.sql(sql)

    sql = """
       create {table_type} table APS_TMP_ETL_RLS_WIP_SOURCE
       (
         lot_id                  VARCHAR(64),
         prodspec_id             VARCHAR(64),
         mainpd_id               VARCHAR(64),
         ope_no                  VARCHAR(64),
         lot_type                VARCHAR(64),
         lot_finished_state      VARCHAR(64),
         cassete_id              VARCHAR(64),
         wafer_qty               DECIMAL,
         priority_class          VARCHAR(64),
         lot_proc_state          VARCHAR(64),
         last_ope_comp_time      VARCHAR(64),
         eqp_id                  VARCHAR(64),
         ope_no_start_time       VARCHAR(64),
         last_process_start_time VARCHAR(64),
         tech_id                 VARCHAR(64),
         customer_id             VARCHAR(64),
         parent_lot_id           VARCHAR(64),
         trans_state             VARCHAR(64),
         FLOWBATCH_ID            VARCHAR(64),
         sub_lot_type            VARCHAR(64),
         lot_inv_state           VARCHAR(64)
       )
       """.format(table_type=table_type)
    duck_db.sql(sql)

    sql = """
     create {table_type} table APS_TMP_ETL_RLS_LOT_TO_EQPG_RECIPE_V
     (
         LOT_ID VARCHAR(64), 
         OPE_NO VARCHAR(64), 
         PROD_ID VARCHAR(64), 
         PLAN_ID VARCHAR(64)
     )
     """.format(table_type=table_type)
    duck_db.sql(sql)

    sql = """
         create {table_type} table APS_TMP_ETL_RLS_FRDRBL_ID_DYB1
         (
             LOT_ID VARCHAR(64), 
             OPE_NO VARCHAR(64), 
             MAINPD_ID VARCHAR(64), 
             DRBL_ID VARCHAR(128)
         )
         """.format(table_type=table_type)
    duck_db.sql(sql)

    sql = """
         create {table_type} table APS_TMP_ETL_RLS_LOT_TO_EQP_V
         (
             LOT_ID VARCHAR(64), 
             OPE_NO VARCHAR(64)
         )
         """.format(table_type=table_type)
    duck_db.sql(sql)

    sql = """
            create {table_type} table APS_TMP_ETL_RLS_LOTSPECIAL_RESULT1
            (
                LOT_ID VARCHAR(64), 
                OPE_NO VARCHAR(64)
            )
            """.format(table_type=table_type)
    duck_db.sql(sql)

    sql = """
           create {table_type} table APS_TMP_ETL_RLS_REWORK_COMING1
           (
               LOT_SON VARCHAR(64), 
               OPE_NO VARCHAR(64)
           )
           """.format(table_type=table_type)
    duck_db.sql(sql)

    sql = """
           create {table_type} table APS_TMP_ETL_RLS_REWORK_COMING_SELF1
           (
               LOT_SON VARCHAR(64), 
               OPE_NO VARCHAR(64)
           )
           """.format(table_type=table_type)
    duck_db.sql(sql)

    sql = """
           create {table_type} table APS_TMP_RLS_RET_FLOW
           (
                RETN_MAINPD_ID VARCHAR(64), 
                RETN_OPE_NO VARCHAR(64), 
                LOT_ID VARCHAR(64), 
                OPE_NO VARCHAR(64), 
                PRODSPEC_ID VARCHAR(64), 
                MAINPD_ID VARCHAR(64)
           )
           """.format(table_type=table_type)
    duck_db.sql(sql)

    sql = """
           create {table_type} table APS_TMP_RLS_RET_FLOW_STEP
           (
                TOOLG_ID    VARCHAR(64), 
                RETN_MAINPD_ID VARCHAR(64), 
                RETN_OPE_NO VARCHAR(64),
                RETN_STEP_ID VARCHAR(64), 
                LOT_ID VARCHAR(64), 
                OPE_NO VARCHAR(64), 
                PRODSPEC_ID VARCHAR(64), 
                MAINPD_ID VARCHAR(64),
                FLOW_RECIPE VARCHAR(64)
           )
           """.format(table_type=table_type)
    duck_db.sql(sql)

    sql = """ 
       create {table_type} table APS_TMP_RLS_RET_FLOW_RESULT
       (
              parent_id          VARCHAR(64),
              lot_id             VARCHAR(64),
              prodspec_id        VARCHAR(64),
              plan_id            VARCHAR(64),
              target_plan_id     VARCHAR(64),
              ope_no             VARCHAR(64),
              ope_seq            VARCHAR(64),
              target_ope_no      VARCHAR(64),
              target_ope_seq     VARCHAR(64),
              target_toolg_id    VARCHAR(64),
              lot_type           VARCHAR(64),
              lot_finished_state VARCHAR(64),
              foup_id            VARCHAR(64),
              wafer_qty          VARCHAR(64),
              lot_status         VARCHAR(64),
              flow_recipe        VARCHAR(64),
              create_time        VARCHAR(64),
              trans_state        VARCHAR(64),
              batch_id           VARCHAR(64),
              sub_lot_type       VARCHAR(64)
       )
           """.format(table_type=table_type)
    duck_db.sql(sql)

    sql = """ 
       create {table_type} table APS_TMP_ETL_RLS_NPW_RECIPE_GROUP1
       (
              tool_id   VARCHAR(64),
              recipe_id VARCHAR(64),
              group_id  VARCHAR(64)
       )
       """.format(table_type=table_type)
    duck_db.sql(sql)

    sql = """ 
         create {table_type} table APS_TMP_ETL_RLS_WIP_RET
         (
              parent_id          VARCHAR(64),
              lot_id             VARCHAR(64),
              prodspec_id        VARCHAR(64),
              plan_id            VARCHAR(64),
              ope_no             VARCHAR(64),
              ope_seq            VARCHAR(64),
              lot_type           VARCHAR(64),
              lot_finished_state VARCHAR(64),
              foup_id            VARCHAR(64),
              wafer_qty          VARCHAR(64),
              lot_status         VARCHAR(64),
              create_time        VARCHAR(64),
              trans_state        VARCHAR(64),
              batch_id           VARCHAR(64),
              sub_lot_type       VARCHAR(64)
         )
         """.format(table_type=table_type)
    duck_db.sql(sql)

    sql = """ 
         create {table_type} table APS_TMP_ETL_RLS_LOT_TO_EQPG_CH_COUNT
         (
              lot_id             VARCHAR(64),
              TARGET_STEP_ID     VARCHAR(64), 
              TARGET_PLAN_ID     VARCHAR(64), 
              PROD_ID            VARCHAR(64), 
              EQP_ID             VARCHAR(64), 
              PHYSICAL_RECIPE_ID VARCHAR(64), 
              CH_COUNT           VARCHAR(64),
              CHAMBER_SET_TWIN   VARCHAR(64)
         )
         """.format(table_type=table_type)
    duck_db.sql(sql)

    sql = """ 
        create {table_type} table APS_TMP_ETL_RLS_LOT_TO_EQPG_PARALLEL_MODE
        (
             tool_id             VARCHAR(64)
        )
        """.format(table_type=table_type)
    duck_db.sql(sql)

    sql = """ 
        create {table_type} table APS_TMP_ETL_RLS_PRODUCT
        (
             prod_id             VARCHAR(64),
             prodg1              VARCHAR(64)
        )
        """.format(table_type=table_type)
    duck_db.sql(sql)

    sql = """ 
        create {table_type} table APS_TMP_ETL_RLS_PH_RTDQTIME
        (
             PLAN_ID         VARCHAR(64), 
             OPE_NO          VARCHAR(64),
             PROD_ID         VARCHAR(64),
             LOT_ID          VARCHAR(64),
             RTD_QTIME       VARCHAR(64),
             REMAIN_QTIME    VARCHAR(64)
        )
        """.format(table_type=table_type)
    duck_db.sql(sql)

    sql = """ 
        create {table_type} table APS_TMP_ETL_RLS_DIFF_BATCH_SIZE_MIX
        (
             EQP_ID    VARCHAR(64), 
             OPE_NO   VARCHAR(64), 
             LOGICAL_RECIPE   VARCHAR(64), 
             MIN_WAFER   VARCHAR(64), 
             MAX_WAFER  VARCHAR(64)
        )
        """.format(table_type=table_type)
    duck_db.sql(sql)

    sql = """ 
        create {table_type} table APS_TMP_ETL_RLS_MEASURE_RULE
        (
            OPE_NO VARCHAR(64),
            PRODSPEC_ID VARCHAR(64),
            PRODG1 VARCHAR(64), 
            PROD_ID VARCHAR(64)
        )
        """.format(table_type=table_type)
    duck_db.sql(sql)

    sql = """ 
       create {table_type} table APS_TMP_ETL_RLS_WIP_MEASURE_COMING_DATE_RULE
       (
            parent_id          VARCHAR(64),
            lot_id             VARCHAR(64),
            prodspec_id        VARCHAR(64),
            plan_id            VARCHAR(64),
            target_plan_id     VARCHAR(64),
            ope_no             VARCHAR(64),
            ope_seq            VARCHAR(64),
            target_ope_no      VARCHAR(64),
            target_ope_seq     VARCHAR(64),
            target_toolg_id    VARCHAR(64),
            lot_type           VARCHAR(64),
            lot_finished_state VARCHAR(64),
            foup_id            VARCHAR(64),
            wafer_qty          decimal,
            lot_status         VARCHAR(64),
            flow_recipe        VARCHAR(64),
            create_time        VARCHAR(64),
            trans_state        VARCHAR(64),
            batch_id           VARCHAR(64),
            sub_lot_type       VARCHAR(64)
       )
        """.format(table_type=table_type)
    duck_db.sql(sql)

    sql = """ 
        create {table_type} table APS_TMP_ETL_RLS_ATP_CASE2
        (
            LOT_ID VARCHAR(64), 
            TOOL_ID VARCHAR(64), 
            PPID VARCHAR(64), 
            OPE_NO VARCHAR(64), 
            TASK_NO VARCHAR(64)
        )
        """.format(table_type=table_type)
    duck_db.sql(sql)

    sql = """ 
       create {table_type} table APS_TMP_ETL_RLS_ATP_CASE1_V1
       (
            EQP_ID VARCHAR(64), 
            OPE_NO VARCHAR(64), 
            PPID VARCHAR(64), 
            TASK_NO VARCHAR(64), 
            prod_id VARCHAR(64), 
            suffix_prod VARCHAR(64),
            SPLIT_FLAG VARCHAR(64),
            SPLIT_WAFER_COUNT VARCHAR(64), 
            CONTROL_PROCESS VARCHAR(64),
            MERGE_OPERATION VARCHAR(64)
       )
       """.format(table_type=table_type)
    duck_db.sql(sql)

    sql = """ 
       create {table_type} table APS_TMP_ETL_RLS_ATP_CASE1_V2
       (
            EQP_ID VARCHAR(64), 
            OPE_NO VARCHAR(64), 
            PPID VARCHAR(64), 
            TASK_NO VARCHAR(64), 
            LOT_ID VARCHAR(64), 
            QTY VARCHAR(64),
            SPLIT_FLAG VARCHAR(64),
            SPLIT_WAFER_COUNT VARCHAR(64), 
            CONTROL_PROCESS VARCHAR(64),
            MERGE_OPERATION VARCHAR(64),
            STEP_ID VARCHAR(64), 
            TARGET_STEP_ID VARCHAR(64)
       )
       """.format(table_type=table_type)
    duck_db.sql(sql)

    sql = """ 
           create {table_type} table APS_TMP_ETL_RLS_ATP_CASE1_V3
           (
                EQP_ID VARCHAR(64), 
                OPE_NO VARCHAR(64), 
                PPID VARCHAR(64), 
                TASK_NO VARCHAR(64), 
                LOT_ID VARCHAR(64), 
                QTY VARCHAR(64),
                SPLIT_FLAG VARCHAR(64),
                SPLIT_WAFER_COUNT VARCHAR(64), 
                CONTROL_PROCESS VARCHAR(64),
                MERGE_OPERATION VARCHAR(64),
                STEP_ID VARCHAR(64), 
                TARGET_STEP_ID VARCHAR(64),
                PROD_ID  VARCHAR(64), 
                TARGET_PLAN_ID  VARCHAR(64)
           )
           """.format(table_type=table_type)
    duck_db.sql(sql)

    sql = """ 
       create {table_type} table APS_TMP_ETL_RLS_ATP_CASE1_V31
       (
            EQP_ID VARCHAR(64), 
            OPE_NO VARCHAR(64), 
            PPID VARCHAR(64), 
            TASK_NO VARCHAR(64), 
            LOT_ID VARCHAR(64)
       )
       """.format(table_type=table_type)
    duck_db.sql(sql)

    sql = """ 
       create {table_type} table APS_TMP_ETL_RLS_ATP_CASE1_V4
       (
            EQP_ID VARCHAR(64), 
            OPE_NO VARCHAR(64), 
            PPID VARCHAR(64), 
            TASK_NO VARCHAR(64), 
            LOT_ID VARCHAR(64),
            STEP_ID VARCHAR(64), 
            TARGET_STEP_ID VARCHAR(64),
            PROD_ID  VARCHAR(64), 
            TARGET_PLAN_ID  VARCHAR(64)
       )
       """.format(table_type=table_type)
    duck_db.sql(sql)

    sql = """ 
       create {table_type} table APS_TMP_ETL_RLS_ATP_CASE1_V41
       (
            EQP_ID VARCHAR(64), 
            OPE_NO VARCHAR(64), 
            PPID VARCHAR(64), 
            TASK_NO VARCHAR(64), 
            LOT_ID VARCHAR(64)
       )
       """.format(table_type=table_type)
    duck_db.sql(sql)

    sql = """ 
      create {table_type} table APS_TMP_ETL_RLS_ATP_CASE1_V5
      (
           EQP_ID VARCHAR(64), 
           OPE_NO VARCHAR(64), 
           PPID VARCHAR(64), 
           TASK_NO VARCHAR(64), 
           LOT_ID VARCHAR(64)
      )
      """.format(table_type=table_type)
    duck_db.sql(sql)

    sql = """
         create {table_type} table APS_TMP_ETL_RLS_FLOW_DYB_RETURN
         (
            LOT_ID   VARCHAR(64), 
            OPENO   VARCHAR(64), 
            DYB_MAINPD_ID    VARCHAR(64), 
            MAINPD_ID   VARCHAR(64), 
            RETURN_OPE_NO   VARCHAR(64)
         )
         """.format(table_type=table_type)
    duck_db.sql(sql)

    sql = """
        create {table_type} table APS_TMP_ETL_RLS_MEASURE_COMING_RATE
        (
           PRODSPEC_ID   VARCHAR(64), 
           OPE_NO   VARCHAR(64)
        )
        """.format(table_type=table_type)
    duck_db.sql(sql)

    sql = """ 
          create {table_type} table APS_TMP_ETL_RLS_MEASURE_COMING_SKIP
          (
               parent_id          VARCHAR(64),
               lot_id             VARCHAR(64),
               prodspec_id        VARCHAR(64),
               plan_id            VARCHAR(64),
               target_plan_id     VARCHAR(64),
               ope_no             VARCHAR(64),
               ope_seq            VARCHAR(64),
               target_ope_no      VARCHAR(64),
               target_ope_seq     VARCHAR(64),
               target_toolg_id    VARCHAR(64),
               lot_type           VARCHAR(64),
               lot_finished_state VARCHAR(64),
               foup_id            VARCHAR(64),
               wafer_qty          decimal,
               lot_status         VARCHAR(64),
               flow_recipe        VARCHAR(64),
               create_time        VARCHAR(64),
               trans_state        VARCHAR(64),
               batch_id           VARCHAR(64),
               sub_lot_type       VARCHAR(64),
               mpd_id             VARCHAR(64),
               user_flag          VARCHAR(1),
               recipe_flag        VARCHAR(1),
               eqp_id             VARCHAR(64),
               inhibit_flag       VARCHAR(1)
          )
              """.format(table_type=table_type)
    duck_db.sql(sql)

    sql = """
           create {table_type} table APS_TMP_ETL_RLS_CROSS_LOT
           (
              LOT_ID   VARCHAR(64), 
              TARGET_STEP_ID   VARCHAR(64), 
              dst_mainpd_id    VARCHAR(64) 
           )
           """.format(table_type=table_type)
    duck_db.sql(sql)

