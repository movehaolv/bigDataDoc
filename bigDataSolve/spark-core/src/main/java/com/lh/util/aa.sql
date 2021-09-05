select
 a.click_product_id
from user_visit_action a
where a.click_product_id  -1
limit 10;

    select
       a.*,
    from user_visit_action a
    join product_info p on a.click_product_id = p.product_id
    join city_info c on a.city_id = c.city_id
    where a.click_product_id > -1

  select
      *,
      rank() over( partition by area order by clickCnt desc ) as rank
  from t2

   select
       *
   from t3 where rank <= 3

