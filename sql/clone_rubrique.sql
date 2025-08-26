-- "CD08 - Les Ardennes"	: source : "organizationId" = '0883c467-0dbc-4765-99ec-11d7dca99338';
-- "SMPRR" : dest : "organizationId" = 'db81f5d4-d403-4885-916a-b07c13814e43';



-- id of new sh catalog
select name,id from public.catalog where "organizationId" = 'db81f5d4-d403-4885-916a-b07c13814e43';

-- catalog destination
"RELEVES"	'20bc3346-24d6-425b-b172-24c196cf3cd0'

select name,id from public.catalog where "organizationId" = '0883c467-0dbc-4765-99ec-11d7dca99338';
-- catalog source
"SH"	'daf97530-4983-461e-b8e6-d1a7fac72c11'




-- copy rubric

insert into public.rubric (id,created_at,updated_at,name,type,icon, catalog_id,created_by_id,updated_by_id,"organizationId"
,is_referential_landmarks,is_surface_sections,preset_shape_type,code,is_ai,is_reference_sections)

select uuid_generate_v4(),created_at,updated_at,name,type,icon,'20bc3346-24d6-425b-b172-24c196cf3cd0'::uuid as  catalog_id,created_by_id,updated_by_id
,'db81f5d4-d403-4885-916a-b07c13814e43'::uuid as "organizationId"
,is_referential_landmarks,is_surface_sections,preset_shape_type,code,is_ai,is_reference_sections
from public.rubric where catalog_id='daf97530-4983-461e-b8e6-d1a7fac72c11'
and name in ('SH','Dispositif de Retenu')
;

--copy attribut
insert into public.attribute 
(id
 ,updated_at,name,is_optional,unit,icon
 ,rubric_id
 ,created_by_id,updated_by_id,type,origin,"defaultvalue_Lexiconlocalid","defaultvalue_Text","defaultvalue_Numeric","defaultvalue_Point","defaultvalue_Polygon"
,precision,is_editable,is_calculated,is_preset,code,"defaultvalue_Polyline"

)

select uuid_generate_v4() as id,t3.updated_at,t3.name,t3.is_optional,t3.unit,icon
,r1.dst_rubric_id as rubric_id
,created_by_id,updated_by_id,type,origin,"defaultvalue_Lexiconlocalid","defaultvalue_Text","defaultvalue_Numeric","defaultvalue_Point","defaultvalue_Polygon"
,precision,is_editable,is_calculated,is_preset,code,"defaultvalue_Polyline"
from public.attribute t3 join 

(
	select t1.name,t1.id as src_rubric_id,t2.id as dst_rubric_id from 
public.rubric t1 join public.rubric t2 on t1.catalog_id='daf97530-4983-461e-b8e6-d1a7fac72c11' and t2.catalog_id='20bc3346-24d6-425b-b172-24c196cf3cd0'
and t1.code = t2.code
and t1.name in ('SH','Dispositif de Retenu')

) r1
on t3.rubric_id =r1.src_rubric_id





-- copy reference

insert into public.reference
(
id
,created_at,updated_at,created_by_id,updated_by_id,key,color
,rubric_id
,name,"isGraded","isDefault"

)

select  uuid_generate_v4() as id 
,created_at,updated_at,created_by_id,updated_by_id,key,color
,r1.dst_rubric_id as rubric_id
,t3.name,"isGraded","isDefault"

from public.reference t3 join
(
	select t1.name,t1.id as src_rubric_id,t2.id as dst_rubric_id from 
public.rubric t1 join public.rubric t2 on t1.catalog_id='daf97530-4983-461e-b8e6-d1a7fac72c11' and t2.catalog_id='20bc3346-24d6-425b-b172-24c196cf3cd0'
and t1.code = t2.code
and t1.name in ('SH','Dispositif de Retenu')
) r1
on t3.rubric_id =r1.src_rubric_id;


-- copy lexique
-- coresponte src_attribute_id and dst_attribute_id

select r1.*,t4.name as dst_attriute_name,t4.id as dst_attribute_id
from 
(
	select t1.name as rubric_name,t1.id as src_rubric_id,t2.id as dst_rubric_id
	,t3.name as src_attribute_name, t3.id as src_attribute_id
	
	from public.rubric t1 
	join public.rubric t2 on t1.catalog_id='daf97530-4983-461e-b8e6-d1a7fac72c11' and t2.catalog_id='20bc3346-24d6-425b-b172-24c196cf3cd0' and t1.code = t2.code
	and t1.name in ('SH','Dispositif de Retenu')
	join public.attribute t3 on t1.id = t3.rubric_id
	
	

) r1 join  public.attribute t4 on t4.rubric_id = r1.dst_rubric_id and t4.name = src_attribute_name

-- insert

insert into public.lexicon_value
(id
,created_at,updated_at,value,icon
,attribute_id
,created_by_id,updated_by_id
,"localId"
)

select 
uuid_generate_v4() as id
,created_at,updated_at,value,icon
,r2.dst_attribute_id as attribute_id
,created_by_id,updated_by_id
,(uuid_generate_v4())::text as "localId"

from public.lexicon_value t5 
join
(
select r1.*,t4.name as dst_attriute_name,t4.id as dst_attribute_id
from 
(
	select t1.name as rubric_name,t1.id as src_rubric_id,t2.id as dst_rubric_id
	,t3.name as src_attribute_name, t3.id as src_attribute_id
	
	from public.rubric t1 
	join public.rubric t2 on t1.catalog_id='daf97530-4983-461e-b8e6-d1a7fac72c11' and t2.catalog_id='20bc3346-24d6-425b-b172-24c196cf3cd0' and t1.code = t2.code
	and t1.name in ('SH','Dispositif de Retenu')
	join public.attribute t3 on t1.id = t3.rubric_id
	
	

) r1 join  public.attribute t4 on t4.rubric_id = r1.dst_rubric_id and t4.name = src_attribute_name
) r2 on t5.attribute_id = r2.src_attribute_id
