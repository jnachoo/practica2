from app.agents.agent import Agente
from app.lector.lector_maersk import LectorMaersk
from app.database.db import DatabaseManager
from app.browser.seleniumdriver import SeleniumWebDriver

from selenium.common.exceptions import InvalidSessionIdException, TimeoutException, ElementClickInterceptedException, NoSuchElementException, WebDriverException
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium.webdriver.remote.webelement import WebElement
from selenium.common.exceptions import TimeoutException

class AgenteMaersk(Agente):

    def __init__(self,  web_driver: SeleniumWebDriver, data: DatabaseManager):
        super().__init__( web_driver, data)
        self.proxy = None

    def click_track(self):
        try:
            track = self.browser.find_element(by=By.CLASS_NAME, value="track__search__button")
            track.click()
            return True
        except (NoSuchElementException , ElementClickInterceptedException, WebDriverException) as e:
            print(f"Error: {e}")
            return False
        
    def generar_url(self, bl):
        url = self.data.get_url(naviera=bl.naviera)
        if len(bl.bl_code) > 9:
            url_bl = url + bl.bl_code[4:13]
        else:
            url_bl = url + bl.bl_code
        return url_bl
    
    def guardar_datos(self, bl, url, bl_data, msg, exito):
        # existen containers
        if exito and bl_data:
            self.data.add_containers(bl_data,bl)

        if exito and bl_data == None:
            self.data.add_revision_exitosa(bl)

        if self.proxy:
            self.data.save_request(bl["id"], url, "202", exito, msg)
        else:
            self.data.save_request(bl["id"], url, 202, exito, msg)
        
    def scrape_bl(self, url, bl, intentos=5):
        status = self.navegar_a(url)
        exito = False
        vez = 1
        bl_data = None
        if status:
            #self.delay(1, 2)
            for i in range(intentos):
                estado = self.check_status()
                if estado == "Existe container":
                    print("Existe BL")
                    lector = LectorMaersk(self.browser.page_source)
                    try:
                        bl_data = lector.extraer_informacion()
                    except Exception as e:
                        print(f"Error: {e}")
                        bl_data = "Error al extraer información del html."
                        exito = True
                        vez = i + 1
                        break
                    if bl_data==None:
                        continue
                    exito = True
                    vez = i + 1
                    break
                elif estado == "Not found":
                    click = self.click_track()
                    print("Hace click en track")
                    self.delay(1, 2)
                    continue
                elif estado == "Pagina cargada sin elementos.":
                    print(estado)
                    break
                else:
                    print(f"Error en el intento {i+1}")
                    self.delay(2, 5)
        else: 
            exito = False
            vez = 0
        return exito, vez, bl_data

        
    def scrape(self, bls):
        if self.web_driver.use_proxy:
            self.proxy = self.get_best_proxy(residential=False)
        self.browser = self.web_driver.open_browser(self.proxy)
        intentos = 5
        uso_proxy = False
        for bl in bls:
            if uso_proxy:
                break
            url = self.generar_url(bl)
            for i in range(2):
                exito, vez, bl_data = self.scrape_bl(url, bl, intentos)
                #import pdb; pdb.set_trace()
                if exito:
                    break
                elif bl_data == None:
                    print("Intentando con otro proxy")
                    self.data.save_request(bl["id"], url, self.proxy["id"], exito, "Not Found. Intentando con proxy residencial.")
                    self.browser.quit()
                    if self.web_driver.use_proxy:
                        self.proxy = self.get_best_proxy(residential=True, exclude=self.proxy["id"])
                    else:
                        self.proxy = None
                    self.browser = self.web_driver.open_browser(self.proxy)
                    uso_proxy = True
                    #import pdb; pdb.set_trace()

                    continue
            if exito:
                mensaje = f"Revisado con éxito en el intento {vez}"
            elif bl_data == "Error al extraer información del html.":
                mensaje = f"Error al extraer información del html."
            else:
                mensaje = f"No se encontró el BL en {intentos} intentos"
            
            
            self.guardar_datos(bl, url, bl_data, mensaje, exito)

        self.browser.quit()
        return 
    
    def check_status(self): #TODO: raise exception
        try:
            # Espera hasta que cualquiera de los elementos esté visible
            print("Esperando elementos...")
            WebDriverWait(self.browser, 10).until(
                lambda browser: browser.find_elements(By.CLASS_NAME, "container__header") or
                                browser.find_elements(By.CLASS_NAME, "track__error") or
                                browser.find_elements(By.XPATH, "//button[contains(text(), 'Decline all')]")
            )
        except TimeoutException:
            # Verifica si la página está completamente cargada
            page_loaded = self.browser.execute_script("return document.readyState") == "complete"
            if page_loaded:
                # Si la página se ha cargado completamente pero ninguno de los elementos esperados aparece,
                # puedes retornar un mensaje específico aquí.
                return "Pagina cargada sin elementos."
            else:
                # Si la página no se ha cargado completamente, maneja esta situación según sea necesario
                return "Timeout esperando a que la página se cargue."

        # Una vez que al menos uno de los elementos esté presente, procede a verificar cuál es
        decline_cookies = self.browser.find_elements(By.XPATH, "//button[contains(text(), 'Decline all')]")
        if decline_cookies and decline_cookies[0].is_displayed():
            decline_cookies[0].click()
            self.delay(1, 2) 

        container = self.browser.find_elements(By.CLASS_NAME, "container__header")
        not_found = self.browser.find_elements(By.CLASS_NAME, "track__error")

        if container and container[0].is_displayed():
            return "Existe container"
        elif not_found and not_found[0].is_displayed():
            return "Not found"
        else:
            return "Unknown status"
