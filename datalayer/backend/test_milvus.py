from kafka_custom import KafkaReader

import time
import json
import base64

import numpy as np

# collection='test_milvus3'

# testCollection = MilvusBackend(host="localhost", port='19530', collection=collection)
# vector = [[0.5884650606571111, 0.9107140822639379, 0.3365558641771125, 0.7906379003354387, 0.5619194344592006, 0.7018733324396148, 0.6266047054210063, 0.5474349852785623, 0.2923404795782578, 0.6278774689105736, 0.3878275752579212, 0.9673957045064566, 0.46258199199742644, 0.9145513485214561, 0.7514030983907262, 0.9325505236402784, 0.041267068882705704, 0.33950874689910626, 0.1517907827654248, 0.49035326132610146, 0.7575507594896786, 0.19831880056222828, 0.7911847633017297, 0.2591191727931872, 0.10476939995735512, 0.06861206690322974, 0.21178807019763424, 0.3046657955345493, 0.014656439259063458, 0.1773448613279054, 0.6670437226835201, 0.4093408504623953, 0.6261512778312939, 0.027713339264737735, 0.1748103576789911, 0.7918605324469269, 0.41679004702699374, 0.5459871488606656, 0.07378211610341634, 0.6987651507073044, 0.20260356054845197, 0.49748761139964404, 0.41208482687858217, 0.08606423523283979, 0.3667854946371042, 0.6896690885552436, 0.6886409709188099, 0.005532295083244931, 0.8936606200131124, 0.43495225817421457, 0.09542710243826469, 0.30404734416098167, 0.171724940241751, 0.8547899721672494, 0.24351249116096663, 0.9783560326882665, 0.906000154294355, 0.1337548470863562, 0.8874909133248317, 0.8342106443536861, 0.08146420120457243, 0.6392578585280978, 0.6755858338275539, 0.3634069749511418, 0.06641267685402119, 0.2301857551877059, 0.19140398861753172, 0.4349033118428963, 0.03320996570210433, 0.4091913940863141, 0.4891169017864061, 0.43721066956612564, 0.5403069355040815, 0.4802986423677357, 0.2264238209400975, 0.5174628033735278, 0.19715624818298838, 0.42355862635565233, 0.06227921088663102, 0.12017219635408116, 0.901233444910294, 0.08192938544978634, 0.5809116184777464, 0.4360944620138614, 0.13344601781457177, 0.1956238072220583, 0.006124907318412087, 0.8196280131644011, 0.8139928823763328, 0.6852960792036048, 0.5438838284107563, 0.09383242905007982, 0.5166080189819738, 0.5036193301401569, 0.27573164701048813, 0.4020432687880696, 0.6914234559342443, 0.07231393641437256, 0.6540206446065668, 0.5681514952853602, 0.6568385438756106, 0.9370978903498693, 0.827810817727373, 0.9681602902360428, 0.49559946982470116, 0.3121463671886392, 0.05707763253960341, 0.7112262102282619, 0.5263602955429444, 0.11404414388899142, 0.23152908380275805, 0.5746523966221291, 0.12164735684699268, 0.12356067660547443, 0.5317839702465539, 0.3828556913663629, 0.698675027973937, 0.7114610593075147, 0.43310945659193323, 0.6266895511108668, 0.01961038370865764, 0.8800713314039363, 0.8382805330062947, 0.7192424214847698, 0.5392176584422559, 0.881349573502531, 0.01068484648839152, 0.4188330171846082, 0.5629303617914463, 0.9678463736950753, 0.2337239120992679, 0.07722359030250137, 0.1389771986796976, 0.6119045391319285, 0.9946692786109952, 0.2574991650177354, 0.2166530132282306, 0.7826711084083684, 0.5032133862929828, 0.3545807629011051, 0.29462852586500377, 0.5536637366861185, 0.2568271102848647, 0.09546567685290142, 0.12928411192740508, 0.14893645158929958, 0.2532712860504285, 0.08171158689475255, 0.7549952078420354, 0.6519589842689423, 0.35064066277970785, 0.12704386377650334, 0.6767109507804316, 0.6396542539141395, 0.43454381941734366, 0.7130279939839046, 0.4090502930255344, 0.48465122181029874, 0.13451618647723462, 0.17381088955450041, 0.14778925846794244, 0.17387568500039674, 0.91919918111024, 0.7902114006250064, 0.21521975349691091, 0.2616917347052109, 0.3338946747287207, 0.0476069753171966, 0.803332947670128, 0.7030282250244758, 0.40749150265123035, 0.03207094449953918, 0.9642232500287554, 0.43088585111397437, 0.8514200680606234, 0.7533694968680179, 0.31005696309883035, 0.9992999085406628, 0.7183811167180103, 0.4048942702191274, 0.8754013071962984
#            , 0.6453358432853563, 0.40928686925706503, 0.39340220818605065, 0.12909876883140115, 0.8380239583651556, 0.4875265923665828, 0.8583986045026565, 0.9905512738809236, 0.8435550908783633, 0.7512979075495466, 0.973190607386748, 0.250101535801201, 0.32706284494023674, 0.9552500990819472, 0.35770009492562826, 0.10098272917096651, 0.18406588494949405, 0.4424670329170075, 0.5076669893696001, 0.6462438748340192, 0.9356525605750621, 0.7441518295015954, 0.40362289385744776, 0.9852578037367861, 0.9024910844840724, 0.6245019804978763, 0.7544619086755248, 0.24286868277279794, 0.6933785287718147, 0.8596891370054545, 0.46064587260689205, 0.5848128357432638, 0.03618211272899474, 0.013520797315398125, 0.16531710635208074, 0.4645869442711815, 0.3526842383646517, 0.4767009837032896, 0.5861662441765191, 0.13841375811135503, 0.8733210121724989, 0.9027867425295459, 0.10535932651935043, 0.2402043445850156, 0.08953726883051716, 0.17097705744282699, 0.43035136873122004, 0.02299810501724686, 0.8327145792938636, 0.9944856750947622, 0.056372600987613986, 0.637628397617837, 0.08562654963878202, 0.7187372725150736, 0.16912563675905756, 0.1821476308839215, 0.5262422289061242, 0.4992950010459323, 0.9289094722708803, 0.9275342547897467, 0.6536333265889903, 0.17638377731106336, 0.4610980435245473, 0.42795928308285724, 0.3840538088473405, 0.8950415513680299, 0.1804332830937494, 0.057393531762048644, 0.26594995758396345, 0.7464234996382202, 0.0649774385151809, 0.31964235500322635, 0.20561752681929468, 0.0037195630174936545, 0.3739252988581129, 0.6223880045082377, 0.4316536525440925, 0.01258795025839432, 0.5340826033722847, 0.5766247720821678, 0.6965396222065202, 0.9680698656603921, 0.1579418220607044, 0.15784932871296842, 0.874808243711393, 0.37899388101068543, 0.25094742058934383, 0.9296267964915712, 0.8123348022015077, 0.8592613865909858, 0.029171673383398877, 0.04275877588549115, 0.47406693225371477, 0.08262013540086144, 0.26777111730290104, 0.3480953668380844, 0.39203168933713683, 0.189539439503829, 0.7516576832423587, 0.5094740572862212, 0.15598678410316613, 0.5328920172850045, 0.1093146860975045, 0.46335164313152055, 0.9684573371773452, 0.3846219155221384, 0.8077155912379178, 0.3875485674334346, 0.6262789811471446, 0.9676967141934126, 0.30414993824802405, 0.5312499466866196, 0.4692550643662613, 0.5257233296178013, 0.030366527742247462, 0.010203269073063681, 0.7139982571188666, 0.29888313660316845, 0.5225267803366342, 0.8753966866472715, 0.6457533508969673, 0.4840625018856921, 0.07089277133593275, 0.5504011524566883, 0.5271966543581141, 0.7164540662758452, 0.010706559815770622, 0.884836926247503, 0.3504902706284654, 0.9511717880939011, 0.7295789672012555, 0.363333491001544, 0.2586795132856241, 0.8342480143905112, 0.7017061750743332, 0.38077597986512357, 0.7466655469205248, 0.7035265408286794, 0.7781423421610739, 0.5593152020133348, 0.056470212121250585, 0.9819225715644464, 0.230819158812283, 0.47300664761115185, 0.010073096023957895, 0.20886451183160104, 0.6122099970662296, 0.42263903910919254, 0.7513596285128216
#            , 0.6513312444757352, 0.055059762262269496, 0.3057843214282049, 0.6439478756915442, 0.4551223740897812, 0.42850189097159763, 0.42781834642834526, 0.6188342916177922, 0.46842674866783585, 0.08693176592210827, 0.4390706082942357, 0.3483753190482347, 0.13655518517351473, 0.5531076195221963, 0.06277553974267802, 0.141004558283798, 0.6284080568539802, 0.6014801400102098, 0.4186457789163486, 0.1285813018100419, 0.5296620169499792, 0.9457031977158641, 0.38721795969531925, 0.9409640534013392, 0.4896866586495653, 0.07921031779213294, 0.972576944378469, 0.2942432820744916, 0.2278363066503899, 0.9269891372605312, 0.8474407442734552, 0.21081401343665773, 0.975434114045868, 0.5017153368335489, 0.12541731293410407, 0.5456946636989725, 0.6726881175941919, 0.3792019324186485, 0.7609768829732927, 0.3387399605469025, 0.18332235373840533, 0.7511517412638223, 0.05760975587406891, 0.4800148223056485, 0.5162679632930424, 0.19886579231936108, 0.7316795618516245, 0.38956550733496365, 0.014171392029309171, 0.5695246784159301, 0.9072909129718322, 0.20106038213812516, 0.6039263543893533, 0.398074030267329, 0.8183250441008523, 0.7263272701793658, 0.6065065340270164, 0.6484749185009507, 0.2563829365771154, 0.33335818763192704, 0.7598976375698343, 0.29793204960022235, 0.1828529019461178, 0.26275527546815025, 0.192966457954183, 0.6855909146474477, 0.6586666882635515, 0.15032088407000266, 0.3818294571851405, 0.5106340837132151, 0.5144297682550067, 0.8722568459601558, 0.35710724698341323, 0.9856724879445544, 0.7210853001231184, 0.0687337531221337, 0.5385025412720678, 0.07776299299140177, 0.39442607002495467, 0.7892014596179048, 0.12651234113462928, 0.6229229303686539, 0.68072410733559, 0.5917981542863446, 0.1845335538995444, 0.07509396624166864, 0.7976830073405814, 0.595052262485595, 0.5719999800067893, 0.527894766919298, 0.6451584334356053, 0.9786426721700014, 0.4489798523455637, 0.8451805494113527, 0.45257030240475105, 0.12798897829997846, 0.3323330234884946, 0.6056739843467969, 0.08544099161108343, 0.5340652745756159, 0.8975523060010423, 0.772168203232134, 0.6791237560751879, 0.2662094398203416, 0.07815575878708769, 0.5144928139307254, 0.7965600231143405, 0.054110410843852486, 0.491625644970905, 0.29032687697303405, 0.19107288028843228, 0.14542067612734244, 0.745488858011002, 0.45553343105730704, 0.0749152669263703, 0.22792418351382948, 0.9040732339391557, 0.03485114915235876, 0.6019267913221574, 0.6197110171773916, 0.05675972711708133, 0.7763861660806249, 0.9351530554046308, 0.7548362909614144, 0.1271895008202606, 0.8981241770764412, 0.09613833400716387, 0.05034534744466468, 0.8073979226627672, 0.6667030680479712, 0.7818207285724346, 0.448110640965376, 0.6292015024824356, 0.7689937495982128, 0.44217432520051736, 0.016635745522135093, 0.20456285015677844, 0.9453983852639862, 0.36390174953049026, 0.2914703005015178, 0.4093287335239626, 0.6336565996489312, 0.07311551919432224, 0.2649319859212067, 0.07190622836468119, 0.14613236371601424, 0.9171971495674788, 0.03641912480494813, 0.558068935828739, 0.8574944739441961, 0.1380251700096201, 0.6097249009406985, 0.15429608422777574, 0.2766430497307131, 0.576164862391611, 0.5761602349416427, 0.4548433921595242, 0.8859926965706909, 0.7495711287036073, 0.7466795553680781, 0.17057814680271355, 0.8010080535777202, 0.6850906465212466, 0.6174002703593989, 0.045983043283932434, 0.025116620566438153, 0.3414371043794644, 0.7447420993574861, 0.7321389501320688, 0.8528189770677016, 0.7575815033623179, 0.33000605063120003, 0.5867367196063814, 0.005338056163012306, 0.06295768846199379, 0.31786728422619315, 0.570981545434359, 0.8335897364411752, 0.858246527452564, 0.6974719700153235, 0.04956636372404721, 0.9122378653009493]]
    
reader = KafkaReader(bootstrap_servers="localhost:9092", topic='track14', group_id='test')

# results = []
# while True:
#     time.sleep(0.5)
#     results = reader.poll(timeout_ms=10000)

#     print(len(results))
#     # for data in datas:
#     #     x = data.value
#     #     results.append(x)
#     reader.commit()


data = reader.poll(timeout_ms=10000)

n = len(data)
# x = data[n-1].value
x = json.loads(data[n-1].value)
print(x['object_image'].shape)


# # print(data[5])
# # print(len(data))
# # # vector = data['value']
# data = json.loads(data[n-1].value)

# vector = [data['data']]
# results = testCollection.search(vector)

# print(results)

# print(data[0].value)
# print(datA['data'])

# data = [
#     [[random.random() for _ in range(512)] for _ in range(500)],
#     ["metadata is metadata" for _ in range(500)]
#     ]

# # print(data)
# testCollection.insert(data)





