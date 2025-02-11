from app.agents.agent import Agente
from app.lector.lector_hapag import LectorHapag
from app.database.db import DatabaseManager
from app.browser.seleniumdriver import SeleniumWebDriver

from selenium.common.exceptions import InvalidSessionIdException, TimeoutException, ElementClickInterceptedException, NoSuchElementException, WebDriverException
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium.webdriver.remote.webelement import WebElement
from selenium.common.exceptions import TimeoutException

from zenrows import ZenRowsClient

class AgenteHapag(Agente):
    def __init__(self,  web_driver: SeleniumWebDriver, data: DatabaseManager):
        super().__init__( web_driver, data)
        self.proxy = ZenRowsClient("fe625ce84f8d2e2d04a8ec5177710814141fdac8")

    def generar_url(self, bl=None, container=None):
        if container:
            url = "https://www.hapag-lloyd.com/en/online-business/track/track-by-booking-solution.html?blno="#self.data.get_url(naviera=bl.naviera, aux=True)
            url_cont= url + container
            return url_cont
        if bl:
            url = "https://www.hapag-lloyd.com/en/online-business/track/track-by-booking-solution.html?blno="#self.data.get_url(naviera="HAPAG-LLOYD")
            url_bl = url + bl.bl_code
            return url_bl
        else:
            return None

    def guardar_datos(self, bl, url, bl_data, msg, caso, manual=False):
        # existen containers
        if bl_data:
            self.data.add_containers(bl_data,bl)

        if manual:
            self.data.revisar_manualmente(bl)

        if self.proxy:
            self.data.save_request(bl["id"], url, "202", caso, msg)
        else:
            self.data.save_request(bl["id"], url, 202, caso, msg)

    def actualizar_datos(self, bl, url, bl_data, msg, caso):
        if bl_data:
            self.data.update_containers(bl_data,bl)

        if self.proxy:
            self.data.save_request(bl["id"], url, "202", caso, msg)
        else:
            self.data.save_request(bl["id"], url, 202, caso, msg)
        

    def scrape(self, bls):
        proxy = self.data.get_proxy_by_id(202)
        self.browser = self.web_driver.open_browser(proxy)
        for bl in bls:
            self.scrape_bl(bl)
        
        return True
    
    def scrape_bl(self,bl):
        url = self.generar_url(bl=bl)
        status = self.navegar_a(url)
        all_data = ""

        # Bucle para navegar a través de las páginas y recoger datos de la tabla
        while True:
            # Encuentra la tabla por su identificador o clase
            table = self.browser.find_element(By.ID, 'tracing_by_booking_f:hl27')  # Ajusta según el identificador de la tabla
            all_data += table.text  # Agrega el texto de la tabla al string total

            # Intenta hacer clic en el botón Siguiente, rompe el bucle si el botón no existe
            try:
                next_button = self.browser.find_element(By.XPATH, "//button[text()='Next']")
                #import pdb; pdb.set_trace()
                next_button.click()
                self.delay(2)  # Espera para que la página cargue, ajusta según la velocidad de la página
            except NoSuchElementException:
                break  # Si no se encuentra el botón 'Siguiente', rompe el bucle

            print(all_data)
            #import pdb; pdb.set_trace()

        


        

        