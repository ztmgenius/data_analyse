CREATE OR REPLACE FUNCTION "wx_work"."get_department_user"("deptid" _int4)
  RETURNS TABLE("dept_id" int4, "dept_fullname" varchar, "dept_name" varchar, "userid" varchar, "name" varchar, "mobile" varchar, "position" varchar, "gender" int2, "email" varchar) AS $BODY$

BEGIN
    RETURN  QUERY select X.main_department dept_id,Y.name dept_fullname,Y.tree_name dept_name,X.userid,X.name,X.mobile,X.position,x.gender,x.email from wx_work.ods_department_user X,
        (WITH RECURSIVE T (ID, name, TREE_name,parentid, PATH, DEPTH)  AS (
            SELECT A.ID, A.name, A.name as TREE_name,A.parentid, ARRAY[A.ID] AS PATH, 1 AS DEPTH
            FROM wx_work.ods_department A
            WHERE A.ID = any(deptid)
            UNION ALL
            SELECT  D.ID, CAST (T.name||'/'||D.name	AS VARCHAR(255)) AS name, CAST (repeat(' ',T.DEPTH*5)||D.name	AS VARCHAR(255)) AS TREE_name,D.parentid, T.PATH || D.ID, T.DEPTH + 1 AS DEPTH
            FROM wx_work.ods_department D
            JOIN T ON D.parentid = T.ID
            )
            SELECT T.ID, T.name,T.TREE_name, T.parentid, T.PATH, T.DEPTH FROM T
        ORDER BY PATH ) Y
        where X.main_department = Y.id order by Y.name;
END $BODY$
  LANGUAGE plpgsql VOLATILE
  COST 100
  ROWS 1000

CREATE OR REPLACE FUNCTION "wx_work"."get_department"("dept_id" _int4)
  RETURNS TABLE("id" int4, "name" varchar, "tree_name" varchar, "parentid" int4, "path" _int4, "depth" int4) AS $BODY$

BEGIN
    RETURN  QUERY
        WITH RECURSIVE T (ID, name, TREE_name,parentid, PATH, DEPTH)  AS (
            SELECT A.ID, A.name, A.name as TREE_name,A.parentid, ARRAY[A.ID] AS PATH, 1 AS DEPTH
            FROM ods_department A
            WHERE A.ID = any(dept_id)
            UNION ALL
            SELECT  D.ID, CAST (T.name||'/'||D.name	AS VARCHAR(255)) AS name, CAST (repeat(' ',T.DEPTH*5)||D.name	AS VARCHAR(255)) AS TREE_name,D.parentid, T.PATH || D.ID, T.DEPTH + 1 AS DEPTH
            FROM ods_department D
            JOIN T ON D.parentid = T.ID
            )
            SELECT T.ID, T.name,T.TREE_name, T.parentid, T.PATH, T.DEPTH FROM T
        ORDER BY PATH ;

    sdf:= asd
END $BODY$
  LANGUAGE plpgsql VOLATILE
  COST 100
  ROWS 1000

CREATE OR REPLACE FUNCTION "wx_work"."get_mall_department"("mid" int4)
  RETURNS TABLE("id" int4, "name" varchar, "tree_name" varchar, "parentid" int4, "path" _int4, "depth" int4) AS $BODY$
DECLARE dept_id int4[] ;
BEGIN

  dept_id:=array(SELECT b.dept_id FROM	wx_work.dwd_mall_dept b WHERE	b.mid = $1);
  RETURN query SELECT a.id,a.name,a.tree_name,a.parentid,a.path,a.depth FROM wx_work.get_department(dept_id) a ;

END $BODY$
  LANGUAGE plpgsql VOLATILE
  COST 100
  ROWS 1000