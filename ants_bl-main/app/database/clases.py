class BL:
    def __init__(self, id, bl_code, naviera, fecha_bl=None, etapa=None, estado=None, proxima_revision=None, manual_pendiente=False
                # pol_locode=None,
                # pod_locode=None,
                # pol_pais=None,
                # pod_pais=None,
                # pol_port=None,
                # pod_port=None,
                # pol_limpio=None,
                # pod_limpio=None
                ):
        self.id = id
        self.bl_code = bl_code
        self.naviera = naviera
        self.fecha_bl = fecha_bl
        self.etapa = etapa
        self.estado = estado
        self.containers = []
        self.paradas = []
        self.url = None
        self.pol = None
        self.pod = None
        self.request_case = None
        self.revision_manual = False
        self.html_descargado = False
        self.proxima_revision = proxima_revision
        self.manual_pendiente = manual_pendiente
        # self.pol_locode = pol_locode
        # self.pod_locode = pod_locode
        # self.pol_pais = pol_pais
        # self.pod_pais = pod_pais
        # self.pol_port = pol_port
        # self.pod_port = pod_port
        # self.pol_limpio = pol_limpio
        # self.pod_limpio = pod_limpio

    def __repr__(self):
        return f"BL({self.bl_code}, {self.naviera}, {self.fecha_bl}, {self.etapa}, {self.estado})"
    
    def __str__(self):
        return f"BL({self.bl_code}, {self.naviera}, {self.fecha_bl}, {self.etapa}, {self.estado})"
    

class Container:
    def __init__(
            self,
            code, 
            size, 
            type, 
            pol=None, 
            pod=None, 
            bl_id=None, 
            peso_kg=None, 
            service=None,
            cop_no=None
            # pol_locode=None,
            # pod_locode=None,
            # pol_pais=None,
            # pod_pais=None,
            # pol_port=None,
            # pod_port=None,
            # pol_limpio=None,
            # pod_limpio=None
            ):
        self.code = code
        self.size = size
        self.type = type
        self.pol = pol
        self.pod = pod
        self.bl_id = bl_id
        self.peso_kg = peso_kg
        self.service = service
        self.cop_no = cop_no
        # self.pol_locode = pol_locode
        # self.pod_locode = pod_locode
        # self.pol_pais = pol_pais
        # self.pod_pais = pod_pais
        # self.pol_port = pol_port
        # self.pod_port = pod_port
        # self.pol_limpio = pol_limpio
        # self.pod_limpio = pod_limpio


    def __repr__(self):
        return f"Container({self.code}, {self.size}, {self.type}, {self.pol}, {self.pod}, {self.bl_id}, {self.peso_kg}, {self.service}, {self.cop_no})"
    
    def __str__(self):
        return f"Container({self.code}, {self.size}, {self.type}, {self.pol}, {self.pod}, {self.bl_id}, {self.peso_kg}, {self.service}, {self.cop_no})"
    
class Parada:
    def __init__(
            self, 
            lugar,
            pais = None,
            codigo_pais = None,
            locode=None,
            terminal=None,
            status=None,
            fecha=None, 
            orden=None,
            nave_imo=None,
            nave=None,
            viaje=None,
            is_pol=None,
            is_pod=None,
            us_state_code=None
        ):
        self.lugar = lugar
        self.pais = pais
        self.codigo_pais = codigo_pais
        self.locode = locode
        self.terminal = terminal
        self.status = status
        self.nave = nave
        self.viaje = viaje
        self.fecha = fecha
        self.orden = orden
        self.nave_imo = nave_imo
        self.is_pol = is_pol
        self.is_pod = is_pod
        self.us_state_code = us_state_code

    def __repr__(self):
        return f"Parada({self.lugar})"
    
    def __str__(self):
        return f"Parada({self.lugar})"