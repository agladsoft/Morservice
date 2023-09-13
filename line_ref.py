from main import Morservice,logger

class REF(Morservice):

    def get_delta_teu_ref(self):
        logger.info('Получение значения в delta_teo_ref из nle_cross')
        result = self.client.query(
            "SELECT teu_delta FROM nle_cross nc where `month` = 5 and `year` = 2023 and direction = 'import' and is_ref = True and is_empty = 0")
        delta_teu = result.result_rows[0][0] if result.result_rows else 0
        return delta_teu


    def add_columns_ref(self,data_di):
        logger.info('Заполнение данных по контейнерам 40фт')
        delta_count = data_di['delta_count']
        data_result = []
        for i in range(delta_count):
            data = self.get_data(data_di)
            self.add_container_ref(data)
            data_result.append(data)
        return data_result

    def work_to_ref(self):
        delta_teu = self.get_delta_teu_ref()
        if delta_teu > 0:
            delta_count, data_di = self.get_not_coincidences_in_db_positive(True)
            logger.info('Заполнение данных в таблицу')
            data_list = []
            for index, d in data_di.iterrows():
                data_result = self.add_columns_ref(d)
                if data_result is None:
                    continue
                data_list.append(data_result)
            for data in data_list:
                self.write_to_table(data)

    def add_container_ref(self, data):
        data['container_type'] = 'REF'
        data['container_size'] = 40

if __name__ == '__main__':
    REF().work_to_ref()