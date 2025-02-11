

ONE = """with container_repetido as (
select distinct code from containers where size = 'Unknown' and code not in ('NOT_ASSIGNED', 'Empty')
), size_type_container as (
select code, min(size) as "size", min(type) as "type" from containers where code in (select * from container_repetido)
group by code
)
select distinct 
b.bl_code as "blMaster",
c.code as "containerNro",
case 
	when c.code in (select distinct code from containers where size = 'Unknown') then concat(sc."nombre_size", ' ', trim(sc."nombre_type"))
	else concat(dc."nombre_size", ' ', trim(dc."nombre_type"))
end 
 as "size",
b.fecha_bl as "eventDate",
2024 as "yyyy",
extract(month from current_date) as "mm"  
from containers c 
left join bls b on b.id = c.bl_id
left join size_type_container s on s.code = c.code
left join dict_containers dc on trim(dc."size") = trim(c.size) and trim(dc."type") = trim(c.type)
left join dict_containers sc on trim(sc."size") = trim(s.size) and trim(sc."type") = trim(s.type)
where extract(month from b.fecha_bl) in (extract(month from current_date) - 2, extract(month from current_date) - 1)
and b.naviera_id = 3"""



HALO = """with container_repetido as (
select distinct code from containers where size = 'Unknown' and code not in ('NOT_ASSIGNED', 'Empty')
), size_type_container as (
select code, min(size) as "size", min(type) as "type" from containers where code in (select * from container_repetido)
group by code
)
select distinct 
b.bl_code as "blmaster",
c.code as "NoContenedor",
case 
	when c.code in (select distinct code from containers where size = 'Unknown') then sc."nombre_size"
	else dc."nombre_size"
end
 as "Tipo",
 case 
	when c.code in (select distinct code from containers where size = 'Unknown') then sc."nombre_type"
	else dc."nombre_type"
end 
 as "Estado",
b.fecha_bl as "Fecha",
2024 as "yyyy",
extract(month from current_date) as "mm"  
from containers c 
left join bls b on b.id = c.bl_id
left join size_type_container s on s.code = c.code
left join dict_containers dc on trim(dc."size") = trim(c.size) and trim(dc."type") = trim(c.type)
left join dict_containers sc on trim(sc."size") = trim(s.size) and trim(sc."type") = trim(s.type)
where extract(month from b.fecha_bl) in (extract(month from current_date) - 2, extract(month from current_date) - 1)
and b.naviera_id = 2"""



CMA = """
with container_repetido as (
select distinct code from containers where size = 'Unknown' and code not in ('NOT_ASSIGNED', 'Empty')
), size_type_container as (
select code, min(size) as "size", min(type) as "type" from containers where code in (select * from container_repetido)
group by code
)
select distinct 
b.bl_code as "blmaster",
c.code as "nroContenedor",
cast(case 
	when c.code in (select distinct code from containers where size = 'Unknown') then sc."nombre_size"
	else dc."nombre_size"
end as integer)
 as "ContainerSize",
 case 
	when c.code in (select distinct code from containers where size = 'Unknown') then sc."nombre_type"
	else dc."nombre_type"
end 
 as "Type",
2024 as "yyyy",
extract(month from current_date) as "mm"  
from containers c 
left join bls b on b.id = c.bl_id
inner join size_type_container s on s.code = c.code
left join dict_containers dc on trim(dc."size") = trim(c.size) and trim(dc."type") = trim(c.type)
left join dict_containers sc on trim(sc."size") = trim(s.size) and trim(sc."type") = trim(s.type)
where extract(month from b.fecha_bl) in (extract(month from current_date) - 2, extract(month from current_date) - 1)
and b.naviera_id = 1
"""


EVERGREEN = """
with container_repetido as (
select distinct code from containers where size = 'Unknown' and code not in ('NOT_ASSIGNED', 'Empty')
), size_type_container as (
select code, min(size) as "size", min(type) as "type" from containers where code in (select * from container_repetido)
group by code
)
select distinct 
b.bl_code as "blmaster",
c.code as "container",
case 
	when c.code in (select distinct code from containers where size = 'Unknown') then sc."nombre_size"
	else dc."nombre_size"
end
 as "size",
 case 
	when c.code in (select distinct code from containers where size = 'Unknown') then sc."nombre_type"
	else dc."nombre_type"
end 
 as "seal_nro",
 b.fecha_bl as "Date",
2024 as "yyyy",
extract(month from current_date) as "mm"  
from containers c 
left join bls b on b.id = c.bl_id
left join size_type_container s on s.code = c.code
left join dict_containers dc on trim(dc."size") = trim(c.size) and trim(dc."type") = trim(c.type)
left join dict_containers sc on trim(sc."size") = trim(s.size) and trim(sc."type") = trim(s.type)
where extract(month from b.fecha_bl) in (extract(month from current_date) - 2, extract(month from current_date) - 1)
and b.naviera_id = 7"""


MAERSK="""
with container_repetido as (
select distinct code from containers where size = 'Unknown' and code not in ('NOT_ASSIGNED', 'Empty')
), size_type_container as (
select code, min(size) as "size", min(type) as "type" from containers where code in (select * from container_repetido)
group by code
)
select distinct 
b.bl_code as "blMaster",
c.code as "contenedor",
case 
	when c.code in (select distinct code from containers where size = 'Unknown') then sc."nombre_size"
	else dc."nombre_size"
end
 as "size",
 b.fecha_bl as "eta",
2024 as "yyyy",
extract(month from current_date) as "mm"  
from containers c 
left join bls b on b.id = c.bl_id
left join size_type_container s on s.code = c.code
left join dict_containers dc on trim(dc."size") = trim(c.size) and trim(dc."type") = trim(c.type)
left join dict_containers sc on trim(sc."size") = trim(s.size) and trim(sc."type") = trim(s.type)
where extract(month from b.fecha_bl) in (extract(month from current_date) - 2, extract(month from current_date) - 1)
and b.naviera_id = 4"""


MSC = """
with container_repetido as (
select distinct code from containers where size = 'Unknown' and code not in ('NOT_ASSIGNED', 'Empty')
), size_type_container as (
select code, min(size) as "size", min(type) as "type" from containers where code in (select * from container_repetido)
group by code
)
select distinct 
b.bl_code as "blmaster",
c.code as "containerNumber",
case 
	when c.code in (select distinct code from containers where size = 'Unknown') then concat(sc."nombre_size", ' ', trim(sc."nombre_type"))
	else concat(dc."nombre_size", ' ', trim(dc."nombre_type"))
end 
 as "containerType",
2024 as "yyyy",
extract(month from current_date) as "mm"  
from containers c 
left join bls b on b.id = c.bl_id
left join size_type_container s on s.code = c.code
left join dict_containers dc on trim(dc."size") = trim(c.size) and trim(dc."type") = trim(c.type)
left join dict_containers sc on trim(sc."size") = trim(s.size) and trim(sc."type") = trim(s.type)
where extract(month from b.fecha_bl) in (extract(month from current_date) - 2, extract(month from current_date) - 1)
and b.naviera_id = 5"""


COSCO = """
with container_repetido as (
select distinct code from containers where size = 'Unknown' and code not in ('NOT_ASSIGNED', 'Empty')
), size_type_container as (
select code, min(size) as "size", min(type) as "type" from containers where code in (select * from container_repetido)
group by code
)
select distinct 
b.bl_code as "blmaster",
c.code as "ContainerNo",
case 
	when c.code in (select distinct code from containers where size = 'Unknown') then sc."nombre_size"
	else dc."nombre_size"
end
 as "SizeType",
 case 
	when c.code in (select distinct code from containers where size = 'Unknown') then sc."nombre_type"
	else dc."nombre_type"
end 
 as "SealNo",
2024 as "yyyy",
extract(month from current_date) as "mm"  
from containers c 
left join bls b on b.id = c.bl_id
left join size_type_container s on s.code = c.code
left join dict_containers dc on trim(dc."size") = trim(c.size) and trim(dc."type") = trim(c.type)
left join dict_containers sc on trim(sc."size") = trim(s.size) and trim(sc."type") = trim(s.type)
where extract(month from b.fecha_bl) in (extract(month from current_date) - 2, extract(month from current_date) - 1)
and b.naviera_id = 6"""

ZIM = """
with container_repetido as (
select distinct code from containers where size = 'Unknown' and code not in ('NOT_ASSIGNED', 'Empty')
), size_type_container as (
select code, min(size) as "size", min(type) as "type" from containers where code in (select * from container_repetido)
group by code
)
select distinct 
b.bl_code as "blmaster",
c.code as "ContainerNo",
case 
	when c.code in (select distinct code from containers where size = 'Unknown') then sc."nombre_size"
	else dc."nombre_size"
end
 as "SizeType",
 case 
	when c.code in (select distinct code from containers where size = 'Unknown') then sc."nombre_type"
	else dc."nombre_type"
end 
 as "SealNo",
2024 as "yyyy",
extract(month from current_date) as "mm"  
from containers c 
left join bls b on b.id = c.bl_id
left join size_type_container s on s.code = c.code
left join dict_containers dc on trim(dc."size") = trim(c.size) and trim(dc."type") = trim(c.type)
left join dict_containers sc on trim(sc."size") = trim(s.size) and trim(sc."type") = trim(s.type)
where extract(month from b.fecha_bl) in (extract(month from current_date) - 2, extract(month from current_date) - 1)
and b.naviera_id = 9
"""


WANHAILINES="""
with container_repetido as (
select distinct code from containers where size = 'Unknown' and code not in ('NOT_ASSIGNED', 'Empty')
), size_type_container as (
select code, min(size) as "size", min(type) as "type" from containers where code in (select * from container_repetido)
group by code
)
select distinct 
b.bl_code as "blmaster",
c.code as "CtnrNo",
case 
	when c.code in (select distinct code from containers where size = 'Unknown') then concat(sc."nombre_size", ' ', trim(sc."nombre_type"))
	else concat(dc."nombre_size", ' ', trim(dc."nombre_type"), ' ', dc."dryreef")
end 
 as "Description",
2024 as "yyyy",
extract(month from current_date) as "mm"  
from containers c 
left join bls b on b.id = c.bl_id
left join size_type_container s on s.code = c.code
left join dict_containers dc on trim(dc."size") = trim(c.size) and trim(dc."type") = trim(c.type)
left join dict_containers sc on trim(sc."size") = trim(s.size) and trim(sc."type") = trim(s.type)
where extract(month from b.fecha_bl) in (extract(month from current_date) - 2, extract(month from current_date) - 1)
and b.naviera_id = 10
"""


PIL = """
with container_repetido as (
select distinct code from containers where size = 'Unknown' and code not in ('NOT_ASSIGNED', 'Empty')
), size_type_container as (
select code, min(size) as "size", min(type) as "type" from containers where code in (select * from container_repetido)
group by code
)
select distinct 
b.bl_code as "blmaster",
c.code as "ContainerNo",
case 
	when c.code in (select distinct code from containers where size = 'Unknown') then sc."nombre_size"
	else dc."nombre_size"
end
 as "SizeType",
 case 
	when c.code in (select distinct code from containers where size = 'Unknown') then sc."nombre_type"
	else dc."nombre_type"
end 
 as "SealNo",
2024 as "yyyy",
extract(month from current_date) as "mm"  
from containers c 
left join bls b on b.id = c.bl_id
left join size_type_container s on s.code = c.code
left join dict_containers dc on trim(dc."size") = trim(c.size) and trim(dc."type") = trim(c.type)
left join dict_containers sc on trim(sc."size") = trim(s.size) and trim(sc."type") = trim(s.type)
where extract(month from b.fecha_bl) in (extract(month from current_date) - 2, extract(month from current_date) - 1)
and b.naviera_id = 11"""

HMM = """
with container_repetido as (
select distinct code from containers where size = 'Unknown' and code not in ('NOT_ASSIGNED', 'Empty')
), size_type_container as (
select code, min(size) as "size", min(type) as "type" from containers where code in (select * from container_repetido)
group by code
)
select distinct 
b.bl_code as "blmaster",
c.code as "ContainerNo",
case 
	when c.code in (select distinct code from containers where size = 'Unknown') then concat(sc."nombre_size", ' ', trim(sc."nombre_type"))
	else concat(dc."nombre_size", ' ', trim(dc."nombre_type"), ' ', dc."dryreef")
end 
 as "CntrTypeSize",
2024 as "yyyy",
extract(month from current_date) as "mm"  
from containers c 
left join bls b on b.id = c.bl_id
left join size_type_container s on s.code = c.code
left join dict_containers dc on trim(dc."size") = trim(c.size) and trim(dc."type") = trim(c.type)
left join dict_containers sc on trim(sc."size") = trim(s.size) and trim(sc."type") = trim(s.type)
where extract(month from b.fecha_bl) in (extract(month from current_date) - 2, extract(month from current_date) - 1)
and b.naviera_id = 12"""

YANGMING = """
with container_repetido as (
select distinct code from containers where size = 'Unknown' and code not in ('NOT_ASSIGNED', 'Empty')
), size_type_container as (
select code, min(size) as "size", min(type) as "type" from containers where code in (select * from container_repetido)
group by code
)
select distinct 
b.bl_code as "blmaster",
c.code as "contanierNro",
case 
	when c.code in (select distinct code from containers where size = 'Unknown') then sc."nombre_size"
	else dc."nombre_size"
end
 as "size",
 case 
	when c.code in (select distinct code from containers where size = 'Unknown') then sc."nombre_type"
	else dc."nombre_type"
end 
 as "type",
2024 as "yyyy",
extract(month from current_date) as "mm"  
from containers c 
left join bls b on b.id = c.bl_id
left join size_type_container s on s.code = c.code
left join dict_containers dc on trim(dc."size") = trim(c.size) and trim(dc."type") = trim(c.type)
left join dict_containers sc on trim(sc."size") = trim(s.size) and trim(sc."type") = trim(s.type)
where extract(month from b.fecha_bl) in (extract(month from current_date) - 2, extract(month from current_date) - 1)
and b.naviera_id = 13"""

QUERYS = {
    #'ONE': ONE,
    #'HALO': HALO,
    #'CMA_CGM': CMA,
    #'EVERGREEN': EVERGREEN,
    #'MAERSK': MAERSK,
    #'MSC': MSC,
    #'COSCO': COSCO,
    'ZIM': ZIM,
    'WAN_HAI': WANHAILINES,
    'PIL': PIL,
    'HYUNDAI': HMM,
    'YANG_MING': YANGMING
}