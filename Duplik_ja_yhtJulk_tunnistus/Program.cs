using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data;

namespace Duplik_ja_yhtJulk_tunnistus
{
    class Program
    {
        static void Main(string[] args)
        {


            /////////////////////////////////////////////////////////////////
            // Tama on ajastin, joka mittaa kuinka kauan koodin ajo kestaa //
            /////////////////////////////////////////////////////////////////
            //var watch = System.Diagnostics.Stopwatch.StartNew();

            if (args.Length != 1)
            {
                Console.Write("Argumenttien maara on vaara.");

            }
            else
            {

                string server = args[0];

                string connectionString_ods_julkaisut = "Server=" + server + ";Database=julkaisut_ods;Trusted_Connection=true";
                string connectionString_mds_julkaisut = "Server=" + server + ";Database=julkaisut_mds;Trusted_Connection=true";


                //////////////////////////////////
                // VAKIOITA, VAKIOITA, VAKIOITA //
                //////////////////////////////////

                int tarkistusID_sisainen_duplikaatti = 24;
                //int tarkistusID_tyyppiero = 25;
                int tarkistusID_yhteisjulkaisu = 26;


                int tilaKoodi_sisainen_duplikaatti = 0;
                //int tilaKoodi_yhteisjulkaisu = 9;

                /* 
                 * haetaan SA_Julkaisut taulusta vain ne julkaisut, joille julkaisuvuosi > VUOSI. Lisaksi ODS_JulkaisutTMP-tauluun 
                 * lisataan vain ne julkaisut, joille julkaisuvuosi > VUOSI 
                */
                int VUOSI = 2014;

                //---------------------------------------------------------------------------------------------------------------


                // Taalla ovat tarvittavat apufunktiot ja tietokantaoperaatiot
                Apufunktiot apufunktiot = new Apufunktiot();
                Tietokantaoperaatiot tietokantaoperaatiot = new Tietokantaoperaatiot();

                // tama on LevenshteinDistance olio, jonka avulla tutkitaan merkkijonojen yhtäläisyyttä
                //LevenshteinDistance distance = new LevenshteinDistance();

                // tama on vertailu-olio, jonka avulla tutkitaan ovatko kahden julkaisun sarakkeet samat
                Vertailu vertailu = new Vertailu();


                // Tyhjennetaan julkaisut_ods.dbo.ODS_JulkaisutTMP -taulu
                tietokantaoperaatiot.tyhjenna_ODSjulkaisutTMP(server);

                // Populoidaan julkaisut_ods.dbo.ODS_JulkaisutTMP -taulu
                tietokantaoperaatiot.insert_into_ODSjulkaisutTMP(server, VUOSI);

                // Haetaan ODS_JulkaisutTMP -taulusta nimikentat ja muokataan nimia
                SqlConnection conn_ODS_TMP = new SqlConnection(connectionString_ods_julkaisut);
                SqlDataReader reader_ODS_TMP = tietokantaoperaatiot.ODS_JulkaisutTMP_hae_nimet(conn_ODS_TMP);

                while (reader_ODS_TMP.Read())
                {
                    string ods_julkaisunTunnus = (string) reader_ODS_TMP["JulkaisunTunnus"];
                    string ods_julkaisunNimi = reader_ODS_TMP["JulkaisunNimi"] == System.DBNull.Value ? null : (string) reader_ODS_TMP["JulkaisunNimi"];
                    string ods_kustantajanNimi = reader_ODS_TMP["KustantajanNimi"] == System.DBNull.Value ? null : (string)reader_ODS_TMP["KustantajanNimi"];
                    string ods_emojulkaisunNimi = reader_ODS_TMP["EmojulkaisunNimi"] == System.DBNull.Value ? null : (string)reader_ODS_TMP["EmojulkaisunNimi"];
                    string ods_doi = reader_ODS_TMP["DOI"] == System.DBNull.Value ? null : (string)reader_ODS_TMP["DOI"];
                    string ods_lehdenNimi = reader_ODS_TMP["LehdenNimi"] == System.DBNull.Value ? null : (string)reader_ODS_TMP["LehdenNimi"];
                    string ods_edit_julkaisunNimi = ods_julkaisunNimi;
                    string ods_edit_kustantajanNimi = ods_kustantajanNimi;
                    string ods_edit_emojulkaisunNimi = ods_emojulkaisunNimi;
                    string ods_edit_doi = ods_doi;
                    string ods_edit_lehdenNimi = ods_lehdenNimi;
                    // muokataan nimet, jos ovat eri kuin null
                    if ((ods_julkaisunNimi != null) && !(ods_julkaisunNimi.Equals("")))
                    {
                        ods_edit_julkaisunNimi = apufunktiot.muokkaa_nimea(ods_julkaisunNimi);
                    }

                    if ((ods_kustantajanNimi != null) && !(ods_kustantajanNimi.Equals("")))
                    {
                        ods_edit_kustantajanNimi = apufunktiot.muokkaa_nimea(ods_kustantajanNimi);
                    }

                    if ((ods_emojulkaisunNimi != null) && !(ods_emojulkaisunNimi.Equals("")))
                    {
                        ods_edit_emojulkaisunNimi = apufunktiot.muokkaa_nimea(ods_emojulkaisunNimi);
                    }

                    if ((ods_doi != null) && (ods_doi.Length > 2))
                    {
                        ods_edit_doi = apufunktiot.muokkaaDOI(ods_doi);
                    }
                    if ((ods_lehdenNimi != null) && !(ods_lehdenNimi.Equals("")))
                    {
                        ods_edit_lehdenNimi = apufunktiot.muokkaa_nimea(ods_lehdenNimi);
                    }


                    // Paivitetaan sitten ODS_JulkaisutTMP-tauluun nimet, mikali ne ovat erilaisia muokkauksen jalkeen
                    // case 1: JulkaisunNimi
                    if ((ods_edit_julkaisunNimi != null) && !(ods_julkaisunNimi.Equals(ods_edit_julkaisunNimi)))
                    {
                        tietokantaoperaatiot.ODS_JulkaisutTMP_update_nimi(server, ods_julkaisunTunnus, ods_edit_julkaisunNimi, "JulkaisunNimi");
                    }

                    // Case 2: KustantajanNimi
                    if ((ods_edit_kustantajanNimi != null) && !(ods_kustantajanNimi.Equals(ods_edit_kustantajanNimi)))
                    {
                        tietokantaoperaatiot.ODS_JulkaisutTMP_update_nimi(server, ods_julkaisunTunnus, ods_edit_kustantajanNimi, "KustantajanNimi");
                    }

                    // Case 3: EmojulkaisunNimi
                    if ((ods_edit_emojulkaisunNimi != null) && !(ods_emojulkaisunNimi.Equals(ods_edit_emojulkaisunNimi)))
                    {
                        tietokantaoperaatiot.ODS_JulkaisutTMP_update_nimi(server, ods_julkaisunTunnus, ods_edit_emojulkaisunNimi, "EmojulkaisunNimi");
                    }

                    // Case 4: DOI
                    if ((ods_edit_doi != null) && !(ods_doi.Equals(ods_edit_doi)))
                    {
                        tietokantaoperaatiot.ODS_JulkaisutTMP_update_nimi(server, ods_julkaisunTunnus, ods_edit_doi, "DOI");
                    }

                    // Case 5: lehdenNimi
               
                    if ((ods_edit_lehdenNimi != null) && !(ods_lehdenNimi.Equals(ods_edit_lehdenNimi)))
                    {
                        tietokantaoperaatiot.ODS_JulkaisutTMP_update_nimi(server, ods_julkaisunTunnus, ods_edit_lehdenNimi, "LehdenNimi");
                    }

                }

                reader_ODS_TMP.Close();
                conn_ODS_TMP.Close();


                // Haetaan kaikki SA_Julkaisut-taulun julkaisut, joille JulkaisuVuosi > VUOSI
                SqlConnection conn_SA = new SqlConnection(connectionString_ods_julkaisut);
                SqlDataReader reader_SA = tietokantaoperaatiot.SA_julkaisut_hae_kaikki(conn_SA, VUOSI);


                while (reader_SA.Read())
                {

                    string ekaOrganisaatio = (string)reader_SA["OrganisaatioTunnus"];
                    string ekaJulkaisunTunnus = (string)reader_SA["JulkaisunTunnus"];
                  
                    /////////////////////////////////////////////////////////////////////
                    // Jos ekaJulkaisunTunnus kuuluu sellaisten julkaisunTunnusten joukkoon
                    // joita ei haluta tarkastella, niin sitten jatketaan seuraavalta kierrokselta.
                    // Alla on lista niista tunnuksista, jotka skipataan. Tunnukset ovat
                    // sellaisia, jotka on joko vaarin tunnistettu duplikaateiksi tai jotka
                    // ovat aikaisemmin olleet duplikaatteja, mutta jotka on sittemmin korjattu.
                    //////////////////////////////////////////////////////////////////////////////
                    string[] non_duplicates = {"0278927216","0278964316","0278927216","0277490116","0011533615","0011530715","0256229016","0256242916","0277494216","0008462315","0274466515","0008376615","0277535216",
"0008396314","0277499615","0275129616","0254797816","0025833514","0277990016","0277410616","0277413016","0277412616","0277413016","0277408916","0278290616","0256187016","0279436216","0011664916","0028460714","0278290416",
"0277408816","0277565916","0279093216","0279093416","0279093716","0279093816","0279093916","0279094116","0279094216","0279094616","0279094716","0279094816","0279095016","0279095116","0279095316","0279095416","0275815616",
"0279095716","0277862816","0277862916","0277863016","0277863216","0277863516","0277863616","0277863716","0277863816","0279096116","0279096216","0279096416","0279096516","0279096916","0279097016","0279097316","0275816416",
"0279097416","0279097516","0279097616","0279097716","0279097816","0279097916","0279098016","0279098316","0279098516","0279098916","0279099016","0279099216","0279099416","0279099516","0279099716","0279100016","0008592815",
"0279100116","0279100216","0279100516","0279100716","0279100916","0279101016","0279101516","0279101616","0279101716","0279101816","0279101916","0279102016","0279102116","0279102616","0279102816","0279092916","0278097416",
"0279093016","0279093116","0279093316","0279093516","0279093616","0279094016","0279094316","0279094416","0279094516","0279094916","0279095216","0279095516","0279095616","0277863116","0277863316","0277863416","0278111716",
"0277864216","0277864316","0277864416","0279095816","0279095916","0279096016","0279096316","0279096616","0279096716","0279096816","0279097116","0279097216","0279098116","0279098216","0279098416","0279098616","0274481616",
"0279098716","0279098816","0279099116","0279099316","0279099616","0279099816","0279099916","0279100316","0279100416","0279100616","0279100816","0279101116","0279101216","0279101416","0279102216","0279102316","0274484816",
"0279102416","0279102516","0279102716","0277563116","0277563216","0277563416","0277563516","0277563616","0277563716","0277564116","0277564216","0277564316","0277564516","0277564916","0277565016","0277565616","0274476316",
"0277566316","0277566416","0277566616","0277566716","0277566916","0277567216","0277567416","0277567716","0277567816","0277567916","0277568016","0277568116","0277568516","0277568816","0277569016","0277569216","0277557916",
"0277569516","0277569616","0277569716","0277569816","0277569916","0277570016","0274895516","0274895616","0274896016","0277863116","0277863316","0277863416","0277863916","0277864116","0277864216","0277864316","0277680716",
"0277864416","0277864916","0277865116","0277865216","0277865316","0277865516","0277865716","0277865816","0277563016","0277563316","0277563816","0277563916","0277564016","0277564416","0277564616","0277564716","0278323716",
"0277564816","0277565216","0277565316","0277565416","0277565516","0277565716","0277565816","0277565916","0277566016","0277566116","0277566216","0277566516","0277566816","0277567016","0277567116","0277567316","0007214215",
"0277567516","0277567616","0277568216","0277568316","0277568416","0277568616","0277568916","0277569116","0277569316","0277569416","0277570116","0274895416","0274895716","0274895816","0274895916","0274896116","0249849915",
"0274896216","0277862816","0277862916","0277863016","0277863216","0277863516","0277863616","0277863716","0277863816","0277864016","0277864516","0277864616","0277864716","0277864816","0277865016","0277865416","0279371116",
"0277865616","0008800515","0277416416","0277416516","0277422116","0277423016","0277427616","0277436316","0277442916","0277428116","0277431916","0277452516","0277448916","0277449216","0277452716","0279436115","0250125215",
"0280263615","0280263715","0280263815","0274414015","0275614415","0275657715","0275658915","0277385816","0277569816","0010645415","0277708315","0010604315","0277710215","0010624515","0277709415","0010674115","0250079715",
"0277709815","0008740215","0277630216","0008745015","0277594416","0277868316","0277866016","0277865916","0276943016","0026932014","0026898514","0277218916","0026878314","0276944716","0277218016","0277218816","0279374316",
"0250085515","0279410716","0279408416","0279386616","0250117815","0279412416","0279345716","0279371816","0279355416","0279368816","0279377216","0279387516","0279390316","0279410916","0279411616","0279411716","0279413516",
"0280265716","0280265816","0279400216","0280245216","0280245316","0280252716","0280253216","0280236616","0274473816","0277678816","0255242416","0255244016","0256217016","0254795816","0254795916","0255211316","0253132216",
"0252478016","0252237016","0275010116","0253834016","0254804016","0274948216","0253117016","0275080015","0252261315","0006835815","0252224716","0252291616","0252319516","0006719815","0252537016","0252578814","0252643816",
"0252823916","0252895215","0252907916","0252982216","0252820016","0253037516","0253069516","0253194816","0253149516","0253219514","0253834016","0254807716","0254813516","0253222816","0253305415","0274959916","0274936216",
"0275020616","0275022216","0275039616","0275047116","0275052816","0275055616","0275063616","0275069016","0275070316","0275092316","0275092516","0275104116","0275132216","0275132316","0275136316","0275559616","0274990316",
"0275556416","0275819916","0276974916","0276976916","0277219416","0277219616","0277219716","0277223615","0277238416","0006565315","0006622015","0006672615","0006820015","0006831315","0006865715","0006888515","0006889215",
"0006873815","0006881015","0006943715","0006949215","0006949915","0007022915","0007024615","0007030915","0007792715","0007821315","0026612214","0026620314","0026703614","0026827114","0026949114","0006968615","0030514514",
"0252224716","0252254816","0252277116","0252225216","0252238816","0252274716","0252286416","0252289616","0252541816","0252546716","0252556516","0252561116","0252566216","0252588116","0252590016","0252590316","0252593716",
"0252602416","0252524016","0252817116","0252642416","0252644613","0252863616","0252868116","0252874016","0252823916","0252907916","0252880216","0252995016","0252935116","0253022916","0253058716","0253069516","0253154116",
"0253024716","0253151516","0253180616","0253233614","0253271816","0253255816","0253295416","0253305415","0253316816","0254088716","0253330416","0253340316","0254763416","0254804016","0275000816","0274944316","0274974717",
"0275039016","0275104116","0275110916","0275151516","0275020616","0275029016","0275148816","0275559616","0278983716","0278978816","0008965415","0278971116","0278982616","0278974316","0279499916","0279516816","0279527316",
"0279514616","0249992515","0279895616","0279899816","0279935616","0249980315","0279934716","0009800115","0009923015","0010067715","0010175614","0010266215","0010335215","0275986515","0276082316","0276233616","0276268816",
"0276350916","0276352016","0276778416","0277854916","0009399415","0010084315","0010440915","0275935416","0276029616","0276300816","0276641916","0276828715","0277796816","0277829216","0277836216","0009561215","0009879115",
"0010031715","0010142715","0010182215","0010303115","0010443415","0276022416","0276201316","0276237216","0276315416","0276351016","0276485716","0277826116","0277860716","0009994815","0010314915","0275844216","0275980516",
"0276130316","0276579116","0276675716","0277752416","0277800016","0277831416","0009478615","0009548415","0009560615","0010338515","0010432615","0010441215","0010530215","0275933816","0275936016","0275940716","0275968615",
"0275977115","0276038216","0276085016","0276224216","0276555215","0276559816","0276813216","0277720716","0277722415","0277742616","0277752016","0277769815","0277777716","0277792216","0277809616","0277809916","0277823616",
"0277856516","0279276616","0279311516","0279325916","0009495215","0009495615","0009621315","0010457015","0275848515","0275957916","0276022116","0276059216","0276098616","0276205916","0276206016","0276284716","0276420216",
"0276453716","0276665216","0276694315","0276737116","0276817416","0277736716","0277738916","0277760316","0277760816","0277796516","0277837315","0278058016","0009506615","0009545715","0009550715","0275864716","0275977916",
"0275981816","0276010716","0276144016","0276188016","0276217416","0276254515","0276260416","0276441515","0276447615","0276468315","0276545916","0276607116","0276680216","0276680915","0276682116","0276812516","0276861115",
"0276863316","0276864416","0277715816","0277719116","0277724215","0277749315","0277768016","0277778716","0279316516","0279319015","0279326216","0009494715","0009529415","0010557415","0276022216","0276324516","0276385616",
"0276427516","0276429316","0276623916","0276629216","0276854416","0277731916","0277755716","0277801516","0278057716","0278058316","0278059116","0279289816","0279290316","0278329016","0007855815","0250139915","0279074016",
"0250197215","0279187316","0250198215","0279184816","0250184115","0279184916","0250186215","0279184116","0275242016","0277431916","0008114815","0277436316","0008485115","0275345516","0274501116","0274503016","0277998716",
"0277987216","0277987316","0277998816","0274457216","0275151516","0008183315","0277755315","0280479916","0280480816","0026267913","0275312716","0007972915","0275549116","0247732212","0278732916","0278814815","0278815915",
"0278771015","0280480016","0280480916","0280449117","0280525117","0280541417","0280541617","0280577617","0281380517","0281462517","0281463917","0007263315","0281462617","0361482319","0363294019","0361956419"};
                   

                    bool non_duplicate_found = false;


                    foreach (string item in non_duplicates)
                    {
                        if (item.Equals(ekaJulkaisunTunnus))
                        {
                            non_duplicate_found = true;
                            break;
                        }
                    }


                    if (non_duplicate_found)
                    {
                        continue;
                    }




                    string ekaJulkaisunOrgTunnus = (string)reader_SA["JulkaisunOrgTunnus"];
                    int ekaJulkaisuvuosi = (int)reader_SA["JulkaisuVuosi"];
                    string ekaJulkaisunNimi = reader_SA["JulkaisunNimi"] == System.DBNull.Value ? null : (string)reader_SA["JulkaisunNimi"];

                    // muokataan ekaJulkaisunNimea siten, etta jos tulee stop_words -tai stop_chars, niin ne poistetaan
                    if (ekaJulkaisunNimi != null)
                    {
                        ekaJulkaisunNimi = apufunktiot.muokkaa_nimea(ekaJulkaisunNimi);
                    }

                    int ekaJulkaisunTilaKoodi = (int)reader_SA["JulkaisunTilaKoodi"];


                    /////////////////////////////////////////////////////////////////////////////////////
                    // Jos ekaJulkaisunTilaKoodi == -1, niin jatketaan loopia seuraavasta kierroksesta //
                    /////////////////////////////////////////////////////////////////////////////////////
                    if (ekaJulkaisunTilaKoodi == -1)
                    {
                        continue;
                    }

                    string ekatSivut = reader_SA["SivunumeroTeksti"] == System.DBNull.Value ? null : (string)reader_SA["SivunumeroTeksti"];
                    string ekaVolyymi = reader_SA["VolyymiTeksti"] == System.DBNull.Value ? null : (string)reader_SA["VolyymiTeksti"];
                    string ekaNumero = reader_SA["LehdenNumeroTeksti"] == System.DBNull.Value ? null : (string)reader_SA["LehdenNumeroTeksti"];
                    string ekaKustantaja = reader_SA["KustantajanNimi"] == System.DBNull.Value ? null : (string)reader_SA["KustantajanNimi"];
                    string ekaLehdenNimi = reader_SA["LehdenNimi"] == System.DBNull.Value ? null : (string)reader_SA["LehdenNimi"];
                  
                    // muokataan ekaLehdenNimi siten, etta jos tulee stop_words -tai stop_chars, niin ne poistetaan
                    if (ekaLehdenNimi != null)
                    {
                        ekaLehdenNimi = apufunktiot.muokkaa_nimea(ekaLehdenNimi);
                    }

                   
                   
                    // muokataan ekaKustantajaa siten, etta jos tulee stop_words -tai stop_chars, niin ne poistetaan
                    if (ekaKustantaja != null)
                    {
                        ekaKustantaja = apufunktiot.muokkaa_nimea(ekaKustantaja);
                    }

                    string ekaEmojulkaisunNimi = reader_SA["EmojulkaisunNimi"] == System.DBNull.Value ? null : (string)reader_SA["EmojulkaisunNimi"];
                    
                    // muokataan ekaEmojulkaisunNimea siten, etta jos tulee stop_words -tai stop_chars, niin ne poistetaan
                    if (ekaEmojulkaisunNimi != null)
                    {
                        ekaEmojulkaisunNimi = apufunktiot.muokkaa_nimea(ekaEmojulkaisunNimi);
                    }

                    string ekaJulkaisutyyppi = reader_SA["JulkaisutyyppiKoodi"] == System.DBNull.Value ? null : (string)reader_SA["JulkaisutyyppiKoodi"];

                    ///////////////////////////////////////////////////////////////////////////////////
                    // Jos ekaJulkaisutyyppi == null, niin jatketaan loopia seuraavasta kierroksesta //
                    ///////////////////////////////////////////////////////////////////////////////////
                    if (ekaJulkaisutyyppi == null)
                    {
                        continue;
                    }

                    string ekaDOI = reader_SA["DOI"] == System.DBNull.Value ? null : (string)reader_SA["DOI"];

                    // muokataan DOI-tunnusta, mikali se ei ole null ja pituus on yli 2.
                    // Muokkauksella halutaan, etta DOI-tunnus alkaa oikeassa muodossa eli etta ensimmainen merkki on 1 ja toinen 0.
                    if ((ekaDOI != null) && (ekaDOI.Length > 2))
                    {
                        ekaDOI = apufunktiot.muokkaaDOI(ekaDOI);
                    }


                    string ekaLataus_Id = reader_SA["Lataus_ID"] == System.DBNull.Value ? null : (string)reader_SA["Lataus_ID"];

                    // ISSN- ja ISBN -tunnukset. Naiden arvot haetaan kohta
                    string ekaISSN1 = "";
                    string ekaISSN2 = "";

                    string ekaISBN1 = "";
                    string ekaISBN2 = "";


                    // ISSN- ja ISBN -tiedot eivät mene SA_Julkaisut -ja ODS_Julkaisut-tauluihin vaan omiin tauluihinsa, jotka ovat
                    // SA_ISSN, SA_ISBN, ODS_ISSN -ja ODS_ISBN. Haetaan ISSN- ja ISBN -tiedot kyseísista tauluista JulkaisunTunnus-arvon perusteella
                    // Parametrina on julkaisunTunnus seka tietokantayhteys
                    // Tassa haetaan SA_Julkaisut-taulun ISSN -ja ISBN-tunnukset (vahan myohemmin haetaan ODS_Julkaisut-taulun vastaavat)

                    //SqlConnection conn_ISSN_tunnusten_maara_SA = new SqlConnection("Server=dwidvirta;Database=julkaisut_ods;Trusted_Connection=true");
                    int ISSN_tunnusten_maara_SA = tietokantaoperaatiot.count_tunnusten_maara(server, ekaJulkaisunTunnus, "ISSN", "SA");
                    //conn_ISSN_tunnusten_maara_SA.Close();

                    //SqlConnection conn_ISBN_tunnusten_maara_SA = new SqlConnection("Server=dwidvirta;Database=julkaisut_ods;Trusted_Connection=true");
                    int ISBN_tunnusten_maara_SA = tietokantaoperaatiot.count_tunnusten_maara(server, ekaJulkaisunTunnus, "ISBN", "SA");
                    //conn_ISBN_tunnusten_maara_SA.Close();

                    SqlConnection conn_SA_ISSN = new SqlConnection(connectionString_ods_julkaisut);
                    //SqlConnection conn_SA_ISSN = new SqlConnection("Server=dwidvirta;Database=julkaisut_ods;Trusted_Connection=true");
                    SqlDataReader reader_SA_ISSN = tietokantaoperaatiot.hae_tunnus_julkaisulle(conn_SA_ISSN, ekaJulkaisunTunnus, "ISSN", "SA");

                    int laskuri_ISSN = 0;
                    int laskuri_ISBN = 0;

                    // asetetaan issn1 -ja issn2 -tunnukset
                    while (reader_SA_ISSN.Read())
                    {
                        laskuri_ISSN = laskuri_ISSN + 1;

                        if (laskuri_ISSN == 1)
                        {

                            ekaISSN1 = reader_SA_ISSN["ISSN"] == System.DBNull.Value ? null : (string)reader_SA_ISSN["ISSN"];

                            if (ISSN_tunnusten_maara_SA == 1)
                            {
                                break;
                            }

                        }
                        else if (laskuri_ISSN == 2)
                        {

                            ekaISSN2 = reader_SA_ISSN["ISSN"] == System.DBNull.Value ? null : (string)reader_SA_ISSN["ISSN"];
                            break;

                        }
                        else
                        {
                            break;
                        }
                    }

                    //Console.Write(ekaISSN1 + " - " + ekaISSN2 + "\n");

                    reader_SA_ISSN.Close();
                    conn_SA_ISSN.Close();

                    SqlConnection conn_SA_ISBN = new SqlConnection(connectionString_ods_julkaisut);
                    SqlDataReader reader_SA_ISBN = tietokantaoperaatiot.hae_tunnus_julkaisulle(conn_SA_ISBN, ekaJulkaisunTunnus, "ISBN", "SA");

                    // asetetaan isbn1 -ja isbn2 -tunnukset
                    while (reader_SA_ISBN.Read())
                    {
                        laskuri_ISBN = laskuri_ISBN + 1;

                        if (laskuri_ISBN == 1)
                        {

                            ekaISBN1 = reader_SA_ISBN["ISBN"] == System.DBNull.Value ? null : (string)reader_SA_ISBN["ISBN"];

                            if (ISBN_tunnusten_maara_SA == 1)
                            {
                                break;
                            }

                        }
                        else if (laskuri_ISBN == 2)
                        {

                            ekaISBN2 = reader_SA_ISBN["ISBN"] == System.DBNull.Value ? null : (string)reader_SA_ISBN["ISBN"];
                            break;

                        }
                        else
                        {
                            break;
                        }
                    }

                    reader_SA_ISBN.Close();
                    conn_SA_ISBN.Close();


                    //////////////////////////////////////////////////////////////////////////////
                    // Tarkistetaan loytyyko ODS_Julkaisut-taulusta sellaisia julkaisuja, jotka //
                    // ovat yhteisjulkaisuja tai sisaisia duplikaatteja SA_Julkaisut-taulun     //
                    // julkaisun kanssa. Alla on lueteltu ehdot, jotka tarkistetaan. Jos siis   //
                    // loytyy julkaisu ODS_Julkaisut-taulusta, jolle ehdot tayttyvat, niin      //
                    // sitten palautetaan kokonaisluku, joka kertoo mika ehto matchasi (esim    //
                    // 1 = DOI, 2 = ISSN1 + volyymi + numero + sivut jne.                       // 
                    //////////////////////////////////////////////////////////////////////////////

                    // Ehto 0: ei loydy yhteisjulkaisua
                    // Tunnistussääntö 1 - Ehto 1: DOI-tunnus
                    // Tunnistussääntö 2 v1 - Ehto 2: ISSN1 + volyymi + numero + sivut + julkaisun nimi
                    // Tunnistussääntö 2 v2 - Ehto 3: ISSN2 + volyymi + numero + sivut + julkaisun nimi

                    // EDIT 3.11.2016: Ehtoja 4 ja 5 ei tarvita, koska ehtoihin 2 ja 3 tehtiin muutos siten, 
                    // etta niissa on mukana myos julkaisun nimi, jota ei aikaisemmin niissa ollut

                    // Ehto 4: ISSN1 + volyymi + numero + julkaisun nimi
                    // Ehto 5: ISSN2 + volyymi + numero + julkaisun nimi

                    // Tunnistussääntö 3 - Ehto 6: julkaisutyyppi + kustantaja + julkaisun nimi (koskee julkaisutyyppeja C1, D5, E2, pl. Introduction, Esipuhe, Johdanto)
                    // Tunnistussääntö 4 - Ehto 7: emojulkaisun nimi + julkaisun nimi (koskee julkaisutyyppeja A3, A4, B2, B3, D1, D2, D3, E1, pl. Introduction, Esipuhe, Johdanto)
                    // Tunnistussääntö 5 v1 - Ehto 8: ISBN1 + julkaisun nimi
                    // Tunnistussääntö 5 v1 - Ehto 9: ISBN2 + julkaisun nimi

                    // 2/2022 
                    // Tunnistussääntö 6 - Ehto 10: Julkaisutyyppi + julkaisun nimi + lehden nimi + julkaisuvuosi (koskee julkaisutyyppeja D1)
                    // Tunnistussääntö 7 - Ehto 11: Julkaisutyyppi + julkaisun nimi + kustantajan nimi + julkaisuvuosi (koskee julkaisutyyppeja D4)



                    int ODStilaKoodi = 0; // ODS_Julkaisut-alueen tilaKoodi ei saa olla -1 tai 0. Jos on -1 tai 0, niin ko. julkaisua ei oteta mukaan (jos on -1, niin kyseessa on epavalidi julkaisu ja jos on 0, niin kyseessa on jo aikaisemmin sisaiseksi duplikaatiksi identifioitu julkaisu)

                    int loytyy_yhteisjulkaisu = tietokantaoperaatiot.loytyy_yhteisjulkaisu(server, ekaJulkaisunTunnus, ekaDOI, ekaISSN1, ekaISSN2, ekaISBN1, ekaISBN2, ekaVolyymi, ekaNumero, ekatSivut, ekaJulkaisunNimi, ekaJulkaisutyyppi, ekaKustantaja, ekaEmojulkaisunNimi, ekaLehdenNimi, ekaJulkaisuvuosi, ODStilaKoodi);


                    // Jos loydetaan yhteisjulkaisuja, niin nama ovat niiden ODS_alueen julkaisunTunnus ja julkaisunOrgTunnus, organisaatio ja julkaisutyyppi
                    string julkaisunTunnus = "";
                    string julkaisunOrgTunnus = "";
                    string organisaatio = "";
                    string yhteisjulkaisu_Id = "";
                    string julkaisutyyppi = "";
                    string julkaisunNimi = "";  // tata kaytetaan vain DOI-matchauksessa A3-tyypin julkaisuille
                    string DOI = "";    // tata kaytetaan tarkistuksina tapauksille loytyy_yhteisjulkaisu > 1

                    ///////////////////////////////////////////////////////////////////////////////////////
                    // Jos mennaan ensimmaiseen if-lauseeseen alla, niin julkaisu ei ole yhteisjulkaisu. // 
                    // Talloin pitaa tarkistaa, onko [julkaisut_mds].[koodi].[JulkaisunTunnus]-taulussa  //
                    // kyseiselle julkaisulle, Yhteisjulkaisu_ID <> '0'. Jos Yhteisjulkaisu_ID <> '0',   //
                    // niin julkaisulle pitaa asettaa Yhteisjulkaisu_ID = '0'. Lisaksi jos ko.           //
                    // yhteisjulkaisussa on ollut vain kaksi organisaatiota, pitaa myos yhteisjulkaisun  //
                    // toiselle osapuolelle asettaa Yhteisjulkaisu_ID = '0'. Jos yhteisjulkaisussa on    //
                    // enemman kuin kaksi organisaatiota jatetaan muille organisaatioille edelleen       //
                    // Yhteisjulkaisu_ID kuin se oli aluksi.                                             //
                    ///////////////////////////////////////////////////////////////////////////////////////



                    if (loytyy_yhteisjulkaisu == 0)
                    {

                        continue;

                    }


                    //////////////////////////////////////////////////////////////////////////////////
                    // Jos mennaan tanne, niin sitten yhteisjulkaisuja loytyy. Haetaan ODS-alueelta //
                    // datat sen perusteella, milla matchaus tapahtui. Haetaan ODS_Julkaisut-       //
                    // taulusta kentat JulkaisunTunnus, JulkaisunOrgTunnus, OrganisaatioTunnus ja   //
                    // JulkaisutyyppiKoodi.                                                         //
                    //////////////////////////////////////////////////////////////////////////////////


                    else if (loytyy_yhteisjulkaisu == 1) // loytyy match: DOI
                    {

                        //Console.Write("Loytyy DOI match!\n");

                        SqlConnection conn = new SqlConnection(connectionString_ods_julkaisut);
                        SqlDataReader reader = tietokantaoperaatiot.haku_ODS_alueelta_DOI(conn, ekaJulkaisunTunnus, ekaDOI, ODStilaKoodi);

                        while (reader.Read())
                        {

                            julkaisunTunnus = (string)reader["JulkaisunTunnus"];
                            julkaisunOrgTunnus = (string)reader["JulkaisunOrgTunnus"];
                            organisaatio = (string)reader["OrganisaatioTunnus"];
                            //yhteisjulkaisu_Id = tietokantaoperaatiot.hae_Yhteisjulkaisu_ID(julkaisunTunnus);
                            julkaisutyyppi = (string)reader["JulkaisutyyppiKoodi"];
                            julkaisunNimi = (string)reader["JulkaisunNimi"];


                        }

                        reader.Close();
                        conn.Close();

                    }


                    else if (loytyy_yhteisjulkaisu == 2)
                    {

                        julkaisunTunnus = tietokantaoperaatiot.haku_ODS_alueelta_ISSN1_volyymi_numero_sivut_julkaisunNimi(server, ekaJulkaisunTunnus, ekaISSN1, ekaVolyymi, ekaNumero, ekatSivut, ekaJulkaisunNimi, ODStilaKoodi);
                        julkaisunOrgTunnus = tietokantaoperaatiot.hae_julkaisunOrgTunnus_julkaisunTunnuksella(server, julkaisunTunnus);
                        organisaatio = tietokantaoperaatiot.hae_organisaatioTunnus_julkaisunTunnuksella(server, julkaisunTunnus);
                        //yhteisjulkaisu_Id = tietokantaoperaatiot.hae_Yhteisjulkaisu_ID(julkaisunTunnus);
                        julkaisutyyppi = tietokantaoperaatiot.hae_julkaisutyyppi_julkaisunTunnuksella(server, julkaisunTunnus);
                        DOI = tietokantaoperaatiot.hae_DOI_julkaisunTunnuksella(server, julkaisunTunnus);

                    }

                    else if (loytyy_yhteisjulkaisu == 3)
                    {

                        julkaisunTunnus = tietokantaoperaatiot.haku_ODS_alueelta_ISSN2_volyymi_numero_sivut_julkaisunNimi(server, ekaJulkaisunTunnus, ekaISSN2, ekaVolyymi, ekaNumero, ekatSivut, ekaJulkaisunNimi, ODStilaKoodi);
                        julkaisunOrgTunnus = tietokantaoperaatiot.hae_julkaisunOrgTunnus_julkaisunTunnuksella(server, julkaisunTunnus);
                        organisaatio = tietokantaoperaatiot.hae_organisaatioTunnus_julkaisunTunnuksella(server, julkaisunTunnus);
                        //yhteisjulkaisu_Id = tietokantaoperaatiot.hae_Yhteisjulkaisu_ID(julkaisunTunnus);
                        julkaisutyyppi = tietokantaoperaatiot.hae_julkaisutyyppi_julkaisunTunnuksella(server, julkaisunTunnus);
                        DOI = tietokantaoperaatiot.hae_DOI_julkaisunTunnuksella(server, julkaisunTunnus);

                    }


                    else if (loytyy_yhteisjulkaisu == 6)
                    {

                        SqlConnection conn = new SqlConnection(connectionString_ods_julkaisut);
                        SqlDataReader reader = tietokantaoperaatiot.haku_ODS_alueelta_julkTyyppi_julkNimi_kustantaja(conn, ekaJulkaisunTunnus, ekaJulkaisutyyppi, ekaJulkaisunNimi, ekaKustantaja, ODStilaKoodi);

                        while (reader.Read())
                        {

                            julkaisunTunnus = (string)reader["JulkaisunTunnus"];
                            julkaisunOrgTunnus = (string)reader["JulkaisunOrgTunnus"];
                            organisaatio = (string)reader["OrganisaatioTunnus"];
                            //yhteisjulkaisu_Id = tietokantaoperaatiot.hae_Yhteisjulkaisu_ID(julkaisunTunnus);
                            julkaisutyyppi = (string)reader["JulkaisutyyppiKoodi"];

                            DOI = tietokantaoperaatiot.hae_DOI_julkaisunTunnuksella(server, julkaisunTunnus);

                        }

                        reader.Close();
                        conn.Close();

                    }

                    else if (loytyy_yhteisjulkaisu == 7)
                    {

                        SqlConnection conn = new SqlConnection(connectionString_ods_julkaisut);
                        SqlDataReader reader = tietokantaoperaatiot.haku_ODS_alueelta_emoJulkNimi_julkNimi(conn, ekaJulkaisunTunnus, ekaJulkaisutyyppi, ekaEmojulkaisunNimi, ekaJulkaisunNimi, ODStilaKoodi);

                        while (reader.Read())
                        {

                            julkaisunTunnus = (string)reader["JulkaisunTunnus"];
                            julkaisunOrgTunnus = (string)reader["JulkaisunOrgTunnus"];
                            organisaatio = (string)reader["OrganisaatioTunnus"];
                            //yhteisjulkaisu_Id = tietokantaoperaatiot.hae_Yhteisjulkaisu_ID(julkaisunTunnus);
                            julkaisutyyppi = (string)reader["JulkaisutyyppiKoodi"];

                            DOI = tietokantaoperaatiot.hae_DOI_julkaisunTunnuksella(server, julkaisunTunnus);

                        }

                        reader.Close();
                        conn.Close();


                    }

                    else if (loytyy_yhteisjulkaisu == 8)
                    {

                        julkaisunTunnus = tietokantaoperaatiot.haku_ODS_alueelta_ISBN1_julkNimi(server, ekaJulkaisunTunnus, ekaISBN1, ekaJulkaisunNimi, ODStilaKoodi);
                        julkaisunOrgTunnus = tietokantaoperaatiot.hae_julkaisunOrgTunnus_julkaisunTunnuksella(server, julkaisunTunnus);
                        organisaatio = tietokantaoperaatiot.hae_organisaatioTunnus_julkaisunTunnuksella(server, julkaisunTunnus);
                        //yhteisjulkaisu_Id = tietokantaoperaatiot.hae_Yhteisjulkaisu_ID(julkaisunTunnus);
                        julkaisutyyppi = tietokantaoperaatiot.hae_julkaisutyyppi_julkaisunTunnuksella(server, julkaisunTunnus);
                        DOI = tietokantaoperaatiot.hae_DOI_julkaisunTunnuksella(server, julkaisunTunnus);

                    }

                    else if (loytyy_yhteisjulkaisu == 9)
                    {

                        julkaisunTunnus = tietokantaoperaatiot.haku_ODS_alueelta_ISBN2_julkNimi(server, ekaJulkaisunTunnus, ekaISBN2, ekaJulkaisunNimi, ODStilaKoodi);
                        julkaisunOrgTunnus = tietokantaoperaatiot.hae_julkaisunOrgTunnus_julkaisunTunnuksella(server, julkaisunTunnus);
                        organisaatio = tietokantaoperaatiot.hae_organisaatioTunnus_julkaisunTunnuksella(server, julkaisunTunnus);
                        //yhteisjulkaisu_Id = tietokantaoperaatiot.hae_Yhteisjulkaisu_ID(julkaisunTunnus);
                        julkaisutyyppi = tietokantaoperaatiot.hae_julkaisutyyppi_julkaisunTunnuksella(server, julkaisunTunnus);
                        DOI = tietokantaoperaatiot.hae_DOI_julkaisunTunnuksella(server, julkaisunTunnus);

                    }

                    else if (loytyy_yhteisjulkaisu == 10)
                    {
                        SqlConnection conn = new SqlConnection(connectionString_ods_julkaisut);
                        SqlDataReader reader = tietokantaoperaatiot.haku_ODS_alueelta_julkTyyppi_julkNimi_lehdenNimi_julkaisuvuosi(conn, ekaJulkaisunTunnus, ekaJulkaisutyyppi, ekaJulkaisunNimi, ekaLehdenNimi, ekaJulkaisuvuosi, ODStilaKoodi);

                        while (reader.Read())
                        {

                            julkaisunTunnus = (string)reader["JulkaisunTunnus"];
                            julkaisunOrgTunnus = (string)reader["JulkaisunOrgTunnus"];
                            organisaatio = (string)reader["OrganisaatioTunnus"];
                            //yhteisjulkaisu_Id = tietokantaoperaatiot.hae_Yhteisjulkaisu_ID(julkaisunTunnus);
                            julkaisutyyppi = (string)reader["JulkaisutyyppiKoodi"];

                            DOI = tietokantaoperaatiot.hae_DOI_julkaisunTunnuksella(server, julkaisunTunnus);

                        }
                    }
                    else if (loytyy_yhteisjulkaisu == 11)
                    {
                        SqlConnection conn = new SqlConnection(connectionString_ods_julkaisut);
                        SqlDataReader reader = tietokantaoperaatiot.haku_ODS_alueelta_julkTyyppi_julkNimi_kustantajanNimi_julkaisuvuosi(conn, ekaJulkaisunTunnus, ekaJulkaisutyyppi, ekaJulkaisunNimi,  ekaKustantaja, ekaJulkaisuvuosi, ODStilaKoodi);

                        while (reader.Read())
                        {

                            julkaisunTunnus = (string)reader["JulkaisunTunnus"];
                            julkaisunOrgTunnus = (string)reader["JulkaisunOrgTunnus"];
                            organisaatio = (string)reader["OrganisaatioTunnus"];
                            //yhteisjulkaisu_Id = tietokantaoperaatiot.hae_Yhteisjulkaisu_ID(julkaisunTunnus);
                            julkaisutyyppi = (string)reader["JulkaisutyyppiKoodi"];

                            DOI = tietokantaoperaatiot.hae_DOI_julkaisunTunnuksella(server, julkaisunTunnus);

                        }

                        reader.Close();
                        conn.Close();


                    }


                    ///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
                    // Tarkistetaan loytyyko julkaisunTunnus SA-alueelta tilakoodilla -1. Tama vastaa tilannetta, jossa organisaatio on aikaisemmin  //
                    // lahettanut julkaisun virtaan ja nyt myohemmin lahettaa saman julkaisun tilakoodilla -1 eli he haluavat poistaa julkaisun.     //
                    // Mikali tata tarkistusta ei tehda, on mahdollista, etta tulee virheellisia duplikaatti-ilmoituksia. Talloin prosessi menee     //
                    // seuraavasti. Ensin SA-alueen julkaisu tutkitaan duplikaatiksi sen ODS-alueen julkaisun kanssa, jolle tilakoodi = 2. Kuitenkin //
                    // myohemmin SA-alueelle tulee sama ODS-alueen julkaisu uudelleen, mutta tilakoodilla -1. Nain ollen duplikaatti-ilmoitusta      //
                    // ei pitaisi tulla, koska ODS-alueen julkaisu halutaan poistettavan.                                                            //
                    ///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

                    bool julkaisunTunnus_loytyy_SA_alueelta_tilakoodilla_miinus_yksi = tietokantaoperaatiot.julkaisunTunnus_loytyy_SA_alueelta_tilakoodilla_miinus_yksi(server, julkaisunTunnus);

                    if (julkaisunTunnus_loytyy_SA_alueelta_tilakoodilla_miinus_yksi) {
                        continue;
                    }


                    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
                    // Tehdaan DOI-tarkistus tassa vaiheessa. Eli kaytetaan seuraavia paattelyita:
                    //
                    // 1.tapaus: Jos DOI arvot on ilmoitettu ja ne ovat samat, niin kyseessä on duplikaatti
                    // 2.tapaus: Jos DOI arvot on ilmoitettu, mutta ne ovat eri, niin kyseessä ei ole duplikaatti
                    // 3.tapaus: Jos DOI arvo puuttuu toisesta tai kummastakin julkaisusta, niin tarkistetaan muut kuin DOI-ehto
                    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
                    if (loytyy_yhteisjulkaisu > 1)
                    {
                        if ((ekaDOI != null) && !(ekaDOI.Equals("")) && (DOI != null) && !(DOI.Equals("")) && !(ekaDOI.Equals(DOI))) {
                            continue;
                        }
                    }


                    ///////////////////////////////////////////////////////////////////////////////////////////
                    // Tehdaan julkaisutyyppitarkistus kokoomateosten osalta. Saannot ovat seuraavat. Jos on //
                    // C2 ja A3 tai C2 ja B2 samat tiedot, niin ei ole duplikaatti. Mutta sitten taas jos on //
                    // A3 ja B2 samat tiedot, niin se todennakoisesti on duplikaatti.                        //
                    //                                                                                       //               
                    // HUOM! Taman olisi voinut tehda jo aikaisemmin, mutta koska tama paattelysaanto tuli   //
                    // vasta myohemmin, niin se tehdaan tassa kohtaa koodia                                  //
                    ///////////////////////////////////////////////////////////////////////////////////////////

                    ////////////////////////////////////////////////////////////////////////////////////////////////
                    // Jos mennaan tahan haaraan, niin julkaisu on metatietojen perusteella muuten yhteisjulkaisu //
                    // tai sisainen duplikaatti, mutta koska julkaistyyppipari on jokin seuraavista, ei tata      //
                    // pideta matchina vaan erillisina julkaisuina                                                //                        
                    //                                                                                            //
                    //  B3 - C2                                                                                   //
                    //  B3 - D6                                                                                   //
                    //  B3 - E3                                                                                   //
                    //  C2 - A1                                                                                   //
                    //  C2 - A2                                                                                   //
                    //  C2 - A3                                                                                   //
                    //  C2 - A4                                                                                   //
                    //  C2 - B2                                                                                   //
                    //  C2 - D1                                                                                   //
                    //  C2 - D2                                                                                   //
                    //  C2 - D3                                                                                   //
                    //  E3 - A1                                                                                   //
                    //  E3 - A2                                                                                   //
                    //  E3 - A4                                                                                   //
                    //  E3 - D1                                                                                   //
                    //  E3 - D2                                                                                   //
                    //  E3 - D3                                                                                   //
                    //  D6 - A1                                                                                   //
                    //  D6 - A2                                                                                   //
                    //  D6 - A4                                                                                   //
                    //  D6 - D1                                                                                   //
                    //  D6 - D2                                                                                   //
                    //  D6 - D3                                                                                   //
                    //                                                                                            //
                    ////////////////////////////////////////////////////////////////////////////////////////////////
                    if ((ekaJulkaisutyyppi.Equals("B3") && julkaisutyyppi.Equals("C2")) || (ekaJulkaisutyyppi.Equals("C2") && julkaisutyyppi.Equals("B3")) ||
                        (ekaJulkaisutyyppi.Equals("B3") && julkaisutyyppi.Equals("D6")) || (ekaJulkaisutyyppi.Equals("D6") && julkaisutyyppi.Equals("B3")) ||
                        (ekaJulkaisutyyppi.Equals("B3") && julkaisutyyppi.Equals("E3")) || (ekaJulkaisutyyppi.Equals("E3") && julkaisutyyppi.Equals("B3")) ||
                        (ekaJulkaisutyyppi.Equals("C2") && julkaisutyyppi.Equals("A1")) || (ekaJulkaisutyyppi.Equals("A1") && julkaisutyyppi.Equals("C2")) ||
                        (ekaJulkaisutyyppi.Equals("C2") && julkaisutyyppi.Equals("A2")) || (ekaJulkaisutyyppi.Equals("A2") && julkaisutyyppi.Equals("C2")) ||
                        (ekaJulkaisutyyppi.Equals("C2") && julkaisutyyppi.Equals("A3")) || (ekaJulkaisutyyppi.Equals("A3") && julkaisutyyppi.Equals("C2")) ||
                        (ekaJulkaisutyyppi.Equals("C2") && julkaisutyyppi.Equals("A4")) || (ekaJulkaisutyyppi.Equals("A4") && julkaisutyyppi.Equals("C2")) ||
                        (ekaJulkaisutyyppi.Equals("C2") && julkaisutyyppi.Equals("B2")) || (ekaJulkaisutyyppi.Equals("B2") && julkaisutyyppi.Equals("C2")) ||
                        (ekaJulkaisutyyppi.Equals("C2") && julkaisutyyppi.Equals("D1")) || (ekaJulkaisutyyppi.Equals("D1") && julkaisutyyppi.Equals("C2")) ||
                        (ekaJulkaisutyyppi.Equals("C2") && julkaisutyyppi.Equals("D2")) || (ekaJulkaisutyyppi.Equals("D2") && julkaisutyyppi.Equals("C2")) ||
                        (ekaJulkaisutyyppi.Equals("C2") && julkaisutyyppi.Equals("D3")) || (ekaJulkaisutyyppi.Equals("D3") && julkaisutyyppi.Equals("C2")) ||
                        (ekaJulkaisutyyppi.Equals("E3") && julkaisutyyppi.Equals("A1")) || (ekaJulkaisutyyppi.Equals("A1") && julkaisutyyppi.Equals("E3")) ||
                        (ekaJulkaisutyyppi.Equals("E3") && julkaisutyyppi.Equals("A2")) || (ekaJulkaisutyyppi.Equals("A2") && julkaisutyyppi.Equals("E3")) ||
                        (ekaJulkaisutyyppi.Equals("E3") && julkaisutyyppi.Equals("A4")) || (ekaJulkaisutyyppi.Equals("A4") && julkaisutyyppi.Equals("E3")) ||
                        (ekaJulkaisutyyppi.Equals("E3") && julkaisutyyppi.Equals("D1")) || (ekaJulkaisutyyppi.Equals("D1") && julkaisutyyppi.Equals("E3")) ||
                        (ekaJulkaisutyyppi.Equals("E3") && julkaisutyyppi.Equals("D2")) || (ekaJulkaisutyyppi.Equals("D2") && julkaisutyyppi.Equals("E3")) ||
                        (ekaJulkaisutyyppi.Equals("E3") && julkaisutyyppi.Equals("D3")) || (ekaJulkaisutyyppi.Equals("D3") && julkaisutyyppi.Equals("E3")) ||
                        (ekaJulkaisutyyppi.Equals("D6") && julkaisutyyppi.Equals("A1")) || (ekaJulkaisutyyppi.Equals("A1") && julkaisutyyppi.Equals("D6")) ||
                        (ekaJulkaisutyyppi.Equals("D6") && julkaisutyyppi.Equals("A2")) || (ekaJulkaisutyyppi.Equals("A2") && julkaisutyyppi.Equals("D6")) ||
                        (ekaJulkaisutyyppi.Equals("D6") && julkaisutyyppi.Equals("A4")) || (ekaJulkaisutyyppi.Equals("A4") && julkaisutyyppi.Equals("D6")) ||
                        (ekaJulkaisutyyppi.Equals("D6") && julkaisutyyppi.Equals("D1")) || (ekaJulkaisutyyppi.Equals("D1") && julkaisutyyppi.Equals("D6")) ||
                        (ekaJulkaisutyyppi.Equals("D6") && julkaisutyyppi.Equals("D2")) || (ekaJulkaisutyyppi.Equals("D2") && julkaisutyyppi.Equals("D6")) ||
                        (ekaJulkaisutyyppi.Equals("D6") && julkaisutyyppi.Equals("D3")) || (ekaJulkaisutyyppi.Equals("D3") && julkaisutyyppi.Equals("D6")))
                    {
                        continue;
                    }



                    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
                    // Jos mennaan tahan haaraan, niin loytyy match DOIn perusteella, mutta koska kummankin osapuolen                                           // 
                    // julkaisutyyppina on A3, A4, B2, B3, D2, D3, E1 ja julkaisujen nimet eivat matchaa, ei tama ole yhteisjulkaisu tai duplikaattikandidaatti //
                    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
                    if ((loytyy_yhteisjulkaisu == 1) && 
                        (
                        (ekaJulkaisutyyppi.Equals("A3")) && (julkaisutyyppi.Equals("A3")) || (ekaJulkaisutyyppi.Equals("A3")) && (julkaisutyyppi.Equals("A4")) ||
                        (ekaJulkaisutyyppi.Equals("A3")) && (julkaisutyyppi.Equals("B2")) || (ekaJulkaisutyyppi.Equals("A3")) && (julkaisutyyppi.Equals("B3")) ||
                        (ekaJulkaisutyyppi.Equals("A3")) && (julkaisutyyppi.Equals("D2")) || (ekaJulkaisutyyppi.Equals("A3")) && (julkaisutyyppi.Equals("D3")) ||
                        (ekaJulkaisutyyppi.Equals("A3")) && (julkaisutyyppi.Equals("E1")) || (ekaJulkaisutyyppi.Equals("A4")) && (julkaisutyyppi.Equals("A3")) || 
                        (ekaJulkaisutyyppi.Equals("A4")) && (julkaisutyyppi.Equals("A4")) || (ekaJulkaisutyyppi.Equals("A4")) && (julkaisutyyppi.Equals("B2")) || 
                        (ekaJulkaisutyyppi.Equals("A4")) && (julkaisutyyppi.Equals("B3")) || (ekaJulkaisutyyppi.Equals("A4")) && (julkaisutyyppi.Equals("D2")) || 
                        (ekaJulkaisutyyppi.Equals("A4")) && (julkaisutyyppi.Equals("D3")) || (ekaJulkaisutyyppi.Equals("A4")) && (julkaisutyyppi.Equals("E1")) ||
                        (ekaJulkaisutyyppi.Equals("B2")) && (julkaisutyyppi.Equals("A3")) || (ekaJulkaisutyyppi.Equals("B2")) && (julkaisutyyppi.Equals("A4")) ||
                        (ekaJulkaisutyyppi.Equals("B2")) && (julkaisutyyppi.Equals("B2")) || (ekaJulkaisutyyppi.Equals("B2")) && (julkaisutyyppi.Equals("B3")) || 
                        (ekaJulkaisutyyppi.Equals("B2")) && (julkaisutyyppi.Equals("D2")) || (ekaJulkaisutyyppi.Equals("B2")) && (julkaisutyyppi.Equals("D3")) ||
                        (ekaJulkaisutyyppi.Equals("B2")) && (julkaisutyyppi.Equals("E1")) || (ekaJulkaisutyyppi.Equals("B3")) && (julkaisutyyppi.Equals("A3")) ||
                        (ekaJulkaisutyyppi.Equals("B3")) && (julkaisutyyppi.Equals("A4")) || (ekaJulkaisutyyppi.Equals("B3")) && (julkaisutyyppi.Equals("B2")) ||
                        (ekaJulkaisutyyppi.Equals("B3")) && (julkaisutyyppi.Equals("B3")) || (ekaJulkaisutyyppi.Equals("B3")) && (julkaisutyyppi.Equals("D2")) ||
                        (ekaJulkaisutyyppi.Equals("B3")) && (julkaisutyyppi.Equals("D3")) || (ekaJulkaisutyyppi.Equals("B3")) && (julkaisutyyppi.Equals("E1")) ||
                        (ekaJulkaisutyyppi.Equals("D2")) && (julkaisutyyppi.Equals("A3")) || (ekaJulkaisutyyppi.Equals("D2")) && (julkaisutyyppi.Equals("A4")) ||
                        (ekaJulkaisutyyppi.Equals("D2")) && (julkaisutyyppi.Equals("B2")) || (ekaJulkaisutyyppi.Equals("D2")) && (julkaisutyyppi.Equals("B3")) ||
                        (ekaJulkaisutyyppi.Equals("D2")) && (julkaisutyyppi.Equals("D2")) || (ekaJulkaisutyyppi.Equals("D2")) && (julkaisutyyppi.Equals("D3")) ||
                        (ekaJulkaisutyyppi.Equals("D2")) && (julkaisutyyppi.Equals("E1")) || (ekaJulkaisutyyppi.Equals("D3")) && (julkaisutyyppi.Equals("A3")) ||
                        (ekaJulkaisutyyppi.Equals("D3")) && (julkaisutyyppi.Equals("A4")) || (ekaJulkaisutyyppi.Equals("D3")) && (julkaisutyyppi.Equals("B2")) ||
                        (ekaJulkaisutyyppi.Equals("D3")) && (julkaisutyyppi.Equals("B3")) || (ekaJulkaisutyyppi.Equals("D3")) && (julkaisutyyppi.Equals("D2")) ||
                        (ekaJulkaisutyyppi.Equals("D3")) && (julkaisutyyppi.Equals("D3")) || (ekaJulkaisutyyppi.Equals("D3")) && (julkaisutyyppi.Equals("E1")) ||
                        (ekaJulkaisutyyppi.Equals("E1")) && (julkaisutyyppi.Equals("A3")) || (ekaJulkaisutyyppi.Equals("E1")) && (julkaisutyyppi.Equals("A4")) ||
                        (ekaJulkaisutyyppi.Equals("E1")) && (julkaisutyyppi.Equals("B2")) || (ekaJulkaisutyyppi.Equals("E1")) && (julkaisutyyppi.Equals("B3")) ||
                        (ekaJulkaisutyyppi.Equals("E1")) && (julkaisutyyppi.Equals("D2")) || (ekaJulkaisutyyppi.Equals("E1")) && (julkaisutyyppi.Equals("D3")) ||
                        (ekaJulkaisutyyppi.Equals("E1")) && (julkaisutyyppi.Equals("E1"))
                        )
                        && (!(ekaJulkaisunNimi.Equals(julkaisunNimi))))
                    {
                        continue;
                    }


                    /////////////////////////////////////////////////////////////////////////////////////////////////
                    // Jos mennaan tahan haaraan, niin julkaisu on todella yhteisjulkaisu tai sisainen duplikaatti //
                    /////////////////////////////////////////////////////////////////////////////////////////////////
                    else
                    {


                        /////////////////////////////////////////////////////////////////////////////////////////////////
                        // Tutkitaan, onko [julkaisut_mds].[koodi].[JulkaisunTunnus] -taulussa Yhteisjulkaisu_ID = "0" // 
                        /////////////////////////////////////////////////////////////////////////////////////////////////
                        ///////////////////////////////////////////////////////////////////
                        // Jos yhteisjulkaisu_ID = "0", niin talloin SA_Julkaisut-taulun //
                        // julkaisua vastaava ODS_Julkaisut-taulun julkaisu ei ole viela //
                        // yhteisjulkaisu minkaan julkaisun kanssa                       // 
                        ///////////////////////////////////////////////////////////////////



                        //////////////////////////////////////////////////////////////////////////////////////////////////////
                        // Tarkistetaan ensin loytyyko yhteisjulkaisu jo [julkaisut_mds].[koodi].[JulkaisunTunnus]-taulusta //
                        // HUOM! Ainakin tuotannossa pitaisi aina loytya, joten ensimmainen if-lause alla on lahinna        //
                        // testiymparistoa varten, koska ainakaan siella ei aina loytynyt koodi.Julkaisuntunnus-taulusta    //
                        // haluttua julkaisua, vaikka sen siella olisi kuulunut olla.                                       //
                        //////////////////////////////////////////////////////////////////////////////////////////////////////

                        bool yhteisjulkaisu_loytyy_koodi_julkaisunTunnus_taulusta = tietokantaoperaatiot.julkaisu_loytyy_koodi_julkaisunTunnus_taulusta(server, julkaisunTunnus);

                        if (yhteisjulkaisu_loytyy_koodi_julkaisunTunnus_taulusta) // julkaisu loytyy [julkaisut_mds].[koodi].[JulkaisunTunnus]-taulusta
                        {

                            yhteisjulkaisu_Id = tietokantaoperaatiot.hae_Yhteisjulkaisu_ID(server, julkaisunTunnus);    // yhteisjulkaisu_Id

                            if (yhteisjulkaisu_Id.Equals("0"))
                            {

                                /////////////////////////////////////////////////////////
                                // TUTKITAAN SITTEN ONKO KYSEESSA SISAINEN DUPLIKAATTI //
                                /////////////////////////////////////////////////////////

                                /////////////////////////////////////////////////////////////////////////////
                                // Kyseessa on sisainen duplikaatti, mikali ekaOrganisaatio = organisaatio //
                                // Lisataan tapauksessa Tarkistusloki-tauluun tieto siita, etta kyseessa   //
                                // on sisainen duplikaatti.
                                /////////////////////////////////////////////////////////////////////////////

                                if (vertailu.samaOrganisaatio(ekaOrganisaatio, organisaatio))
                                {

                                    //Console.Write(ekaOrganisaatio + " ---" + organisaatio + "\n");

                                    //////////////////////////////////////////////////////////////////////////////////
                                    // EDIT 6.9.2016!                                                               //
                                    // Lisatty alla oleva ehto eli etta ekaJulkaisunOrgTunnus != julkaisunOrgTunnus //
                                    // Tama siita syysta, ettei korkeakoulun aikaisemmin samalla                    //
                                    // julkaisunOrgTunnuksella lahetetty julkaisu mene duplikaatiksi                //
                                    //////////////////////////////////////////////////////////////////////////////////
                                    if (!(ekaJulkaisunOrgTunnus.Equals(julkaisunOrgTunnus)))
                                    {

                                        ////////////////////////////////////////////////////////////////////////////////
                                        // Tarkistetaan etta ekaJulkaisunOrgTunnus-julkaisunOrgTunnus -paria ei       //
                                        // loydy [julkaisut_ods].[dbo].[eiDuplikaatit] -taulusta ko. organisaatiolle. //
                                        // Mikali loytyy, niin kyseessa ei tosiasiallisesti ole duplikaatti vaan      //
                                        // organisaatio on ilmoittanut, etta julkaisu ei ole duplikaatti.             //
                                        // Toinen vaihtoehto miksi julkaisupari voi olla taulussa on se, etta         //
                                        // ne on alun perin identifioitu duplikaatiksi, mutta jalkeenpain todettu,    //
                                        // etta ne eivat ole duplikaatteja.
                                        ////////////////////////////////////////////////////////////////////////////////
                                        bool julkaisupariLoytyyEiDuplikaattiTarkistustaTaulusta = tietokantaoperaatiot.julkaisupari_loytyy_EiDuplikaattiTarkistusta_taulusta(server, ekaOrganisaatio, ekaJulkaisunOrgTunnus, julkaisunOrgTunnus);

                                        if (julkaisupariLoytyyEiDuplikaattiTarkistustaTaulusta == false)
                                        {

                                            //Console.Write(ekaJulkaisunOrgTunnus + "-" + julkaisunOrgTunnus + "\n");
                                            ////////////////////////////////////////////////////////////////////////////////////////
                                            // Lisataan TarkistusLoki-tauluun tieto siita, etta kyseessa on sisainen duplikaatti  //
                                            // Tila = "0" ja Kuvaus = julkaisunOrganisaatioTunnus (ODS_Julkaisut-taulua vastaava //
                                            // tieto)                                                                             //
                                            ////////////////////////////////////////////////////////////////////////////////////////

                                            tietokantaoperaatiot.tarkistusLoki_insert_rivi(server, ekaJulkaisunTunnus, ekaJulkaisunOrgTunnus, ekaOrganisaatio, ekaLataus_Id, tarkistusID_sisainen_duplikaatti, tilaKoodi_sisainen_duplikaatti, julkaisunOrgTunnus);

                                            ///////////////////////////////////////////////////////////////////////////////////////////
                                            // Paivitetaan sitten SA_Julkaisut-tauluun JulkaisunTilaKoodi = 0 (sisainen duplikaatti) //
                                            ///////////////////////////////////////////////////////////////////////////////////////////                                    
                                            tietokantaoperaatiot.julkaisut_update_tilaKoodi(server, tilaKoodi_sisainen_duplikaatti, ekaJulkaisunTunnus, "SA");

                                        }
                       
                                    }

                                }


                                /////////////////////////////////////////////////////////////////////////
                                // Mikali ekaOrganisaatio != organisaatio, niin sitten kyseessa ei ole //
                                // sisainen duplikaatti vaan yhteisjulkaisu. Lisataan TarkistusLoki-   //
                                // tauluun tieto siita, etta kyseessa on yhteisjulkaisu.               //
                                //                                                                     //                                              
                                // Koska kyseessa on uusi yhteisjulkaisu, pitaa luoda uusi             //
                                // yhteisjulkaisu_Id, joka asetetaan koodi.JulkaisunTunnus-tauluun     //
                                /////////////////////////////////////////////////////////////////////////


                                else
                                {

                                    ////////////////////////////////////////////////////////////////////////////////////
                                    // ADDED 2021-03-15
                                    // Tarkistetaan onko julkaisupari jo EiYhteisjulkaisuTarkistusta-taulussa
                                    // Jos on ko. taulussa, niin sitten skipataan yhteisjulkaisutarkistus
                                    ////////////////////////////////////////////////////////////////////////////////
                                    bool julkaisupariLoytyyEiYhteisjulkaisuTarkistustaTaulusta = tietokantaoperaatiot.julkaisupari_loytyy_EiYhteisjulkaisuTarkistusta_taulusta(server, ekaJulkaisunTunnus, julkaisunTunnus);

                                    if (julkaisupariLoytyyEiYhteisjulkaisuTarkistustaTaulusta == false)
                                    {

                                        //////////////////////////////////////////////////////////////////////////////////////////////////////
                                        // haetaan suurin arvo [julkaisut_mds].[koodi].[JulkaisunTunnus]-taulusta ja lisataan siihen arvo 1 //
                                        //////////////////////////////////////////////////////////////////////////////////////////////////////

                                        string suurin_ID = tietokantaoperaatiot.hae_suurin_Yhteisjulkaisu_ID(server);


                                        // [julkaisut_mds].[koodi].[JulkaisunTunnus]-taulussa on jo yhteisjulkaisun ODS_Julkaisut-taulun julkaisu
                                        // Paivitetaan talle julkaisulle uusi Yhteisjulkaisu_ID.
                                        //
                                        // EDIT 13.2.2017! [julkaisut_mds].[koodi].[JulkaisunTunnus]-taulussa pitaisi olla (ainakin tuotannossa)
                                        // myos yhteisjulkaisun SA_Julkaisut -taulun julkaisu

                                        tietokantaoperaatiot.update_Yhteisjulkaisu_ID(server, ekaJulkaisunTunnus, suurin_ID);
                                        tietokantaoperaatiot.update_Yhteisjulkaisu_ID(server, julkaisunTunnus, suurin_ID);

                                        ////////////////////////////////////////////////////////////////////////////////////////
                                        // Lisataan TarkistusLoki-tauluun tieto siita, etta kyseessa on yhteisjulkaisu        //
                                        // ja Kuvaus = suurin_ID (= uusin yhteisjulkaisu_ID)                                  //
                                        ////////////////////////////////////////////////////////////////////////////////////////

                                        tietokantaoperaatiot.tarkistusLoki_insert_rivi(server, ekaJulkaisunTunnus, ekaJulkaisunOrgTunnus, ekaOrganisaatio, ekaLataus_Id, tarkistusID_yhteisjulkaisu, ekaJulkaisunTilaKoodi, suurin_ID);
                                        //tietokantaoperaatiot.tarkistusLoki_insert_rivi(server, ekaJulkaisunTunnus, ekaJulkaisunOrgTunnus, ekaOrganisaatio, ekaLataus_Id, tarkistusID_yhteisjulkaisu, tilaKoodi_yhteisjulkaisu, suurin_ID);

                                        //break;

                                    }


                                }

                            }



                            /////////////////////////////////////////////////////////////////////////////////////////////
                            // Jos mennaan tahan else-haaraan, niin silloin koodi.JulkaisunTunnus-taulun               //
                            // Yhteisjulkaisu_Id > "0". Talloin siis SA_Julkaisut-taulun julkaisua vastaava            //
                            // julkaisu loytyy jo yhteisjulkaisuna ODS_Julkaisut-(ja koodi.JulkaisunTunnus) -taulusta. //   
                            //                                                                                         //  
                            // Tarkistetaan, onko naiden yhteisjulkaisuun osallistuneiden organisaatioiden joukossa    //
                            // SA_Julkaisut-taulun julkaisuun osallistunut organisaatio. Jos organisaatio on           //
                            // jo ODS_Julkaisut-taulun organisaatioiden joukossa, on uusi julkaisu sisainen            // 
                            // duplikaatti. Muuten se on yhteisjulkaisu.                                               //
                            /////////////////////////////////////////////////////////////////////////////////////////////


                            else
                            {


                                ////////////////////////////////////////////////////////////////////////////////////////
                                // Tarkistetaan, onko SA_Julkaisut-taulun julkaisua vastaava organisaatio jo          //
                                // mukana tekijana olemassa olevassa yhteisjulkaisussa. Jos on, niin silloin kyseessa //
                                // on sisainen duplikaatti. Jos ei ole, niin kyseessa on yhteisjulkaisu, jossa        //
                                // on tekijoina enemman kuin kaksi organisaatiota                                     // 
                                ////////////////////////////////////////////////////////////////////////////////////////


                                bool organisaatio_loytyy_jo_yhteisjulkaisusta = tietokantaoperaatiot.organisaatio_loytyy_jo_yhteisjulkaisusta(server, ekaOrganisaatio, yhteisjulkaisu_Id);


                                ///////////////////////////////////////////////////////////////////////////////////////////////////
                                // Mikali edellisessa kohdassa haettu arvo = false, tarkoittaa se sita, etta SA_Julkaisut-taulun //
                                // julkaisun organisaatio ei loydy viela yhteisjulkaisusta. Nain ollen kyseessa ei ole sisainen  //
                                // duplikaatti, vaan yhteisjulkaisu. Lisataan tasta tieto Tarkistusloki-tauluun.                 //
                                ///////////////////////////////////////////////////////////////////////////////////////////////////

                                if (organisaatio_loytyy_jo_yhteisjulkaisusta == false)  // organisaatio ei ole viela mukana yhteisjulkaisussa
                                {

                                    ////////////////////////////////////////////////////////////////////////////////////
                                    // ADDED 2021-03-15
                                    // Tarkistetaan onko julkaisupari jo EiYhteisjulkaisuTarkistusta-taulussa
                                    // Jos on ko. taulussa, niin sitten skipataan yhteisjulkaisutarkistus
                                    ////////////////////////////////////////////////////////////////////////////////
                                    bool julkaisupariLoytyyEiYhteisjulkaisuTarkistustaTaulusta = tietokantaoperaatiot.julkaisupari_loytyy_EiYhteisjulkaisuTarkistusta_taulusta(server, ekaJulkaisunTunnus, julkaisunTunnus);

                                    if (julkaisupariLoytyyEiYhteisjulkaisuTarkistustaTaulusta == false)
                                    {

                                        tietokantaoperaatiot.update_Yhteisjulkaisu_ID(server, ekaJulkaisunTunnus, yhteisjulkaisu_Id);
                                        // Rivin lisaaminen Tarkistusloki-tauluun
                                        tietokantaoperaatiot.tarkistusLoki_insert_rivi(server, ekaJulkaisunTunnus, ekaJulkaisunOrgTunnus, ekaOrganisaatio, ekaLataus_Id, tarkistusID_yhteisjulkaisu, ekaJulkaisunTilaKoodi, yhteisjulkaisu_Id);


                                        //break;

                                    }

                                }


                                ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
                                // Mikali edellisessa kohdassa haettu arvo = true, tarkoittaa se sita, etta SA_Julkaisut-taulun julkaisun organisaatio loytyy //
                                // jo yhteisjulkaisusta. Nain ollen kyseessa on sisainen duplikaatti. Lisataan tasta tieto Tarkistusloki-tauluun.             //
                                ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

                                else
                                //if (organisaatio_loytyy_jo_yhteisjulkaisusta)
                                {

                                    // Haetaan [julkaisut_mds].[koodi].[JulkaisunTunnus]-taulusta se julkaisunTunnus ja julkaisun organisaatiotunnus, jolle
                                    // organisaatio on sama kuin SA_Julkaisut-taulun julkaisulle ja jolle yhteisjulkaisu_ID = yhteisjulkaisu_Id
                                    SqlConnection conn_hae_yhteisjulkaisun_orgTunnus_duplikaatille = new SqlConnection(connectionString_mds_julkaisut);
                                    SqlDataReader hae_yhteisjulkaisun_orgTunnus_duplikaatille = tietokantaoperaatiot.hae_yhteisjulkaisun_orgTunnus_duplikaatille(conn_hae_yhteisjulkaisun_orgTunnus_duplikaatille, ekaOrganisaatio, yhteisjulkaisu_Id);

                                    while (hae_yhteisjulkaisun_orgTunnus_duplikaatille.Read())
                                    {

                                        string julkTunnus = hae_yhteisjulkaisun_orgTunnus_duplikaatille["JulkaisunTunnus"] == System.DBNull.Value ? null : (string)hae_yhteisjulkaisun_orgTunnus_duplikaatille["JulkaisunTunnus"];
                                        string julkOrgTunnus = hae_yhteisjulkaisun_orgTunnus_duplikaatille["JulkaisunOrgTunnus"] == System.DBNull.Value ? null : (string)hae_yhteisjulkaisun_orgTunnus_duplikaatille["JulkaisunOrgTunnus"];

                                        // Tarkistetaan, etta kyseessa ei ole sisainen duplikaatti itsensa kanssa.
                                        // Kyseessa on sisainen duplikaatti itsensa kanssa, mikali ekaJulkaisunTunnus == julkTunnus
                                        if (!ekaJulkaisunTunnus.Equals(julkTunnus))
                                        {

                                            ////////////////////////////////////////////////////////////////////////////////
                                            // Tarkistetaan etta ekaJulkaisunOrgTunnus-julkaisunOrgTunnus -paria ei       //
                                            // loydy [julkaisut_ods].[dbo].[eiDuplikaatit] -taulusta ko. organisaatiolle. //
                                            // Mikali loytyy, niin kyseessa ei tosiasiallisesti ole duplikaatti vaan      //
                                            // organisaatio on ilmoittanut, etta julkaisu ei ole duplikaatti,             //
                                            ////////////////////////////////////////////////////////////////////////////////
                                            bool julkaisupariLoytyyEiDuplikaattiTarkistustaTaulusta = tietokantaoperaatiot.julkaisupari_loytyy_EiDuplikaattiTarkistusta_taulusta(server, ekaOrganisaatio, ekaJulkaisunOrgTunnus, julkOrgTunnus);

                                            if (julkaisupariLoytyyEiDuplikaattiTarkistustaTaulusta == false)
                                            {

                                                // Paivitetaan tilakoodi SA_Julkaisut-tauluun
                                                tietokantaoperaatiot.julkaisut_update_tilaKoodi(server, tilaKoodi_sisainen_duplikaatti, ekaJulkaisunTunnus, "SA");


                                                ////////////////////////////////////////////////////////////////////////////////////////
                                                // Lisataan TarkistusLoki-tauluun tieto siita, etta kyseessa on sisainen duplikaatti  //
                                                // Tila = "0" ja Kuvaus = julkOrgTunnus                                              //
                                                ////////////////////////////////////////////////////////////////////////////////////////

                                                tietokantaoperaatiot.tarkistusLoki_insert_rivi(server, ekaJulkaisunTunnus, ekaJulkaisunOrgTunnus, ekaOrganisaatio, ekaLataus_Id, tarkistusID_sisainen_duplikaatti, tilaKoodi_sisainen_duplikaatti, julkOrgTunnus);

                                            }
                                            
                                        }

                                    }

                                    hae_yhteisjulkaisun_orgTunnus_duplikaatille.Close();
                                    conn_hae_yhteisjulkaisun_orgTunnus_duplikaatille.Close();

                                }

                            }

                        }

                    }
                    

                    // Tanne hypataan jos on tilanne, jossa julkaisu on vaarin identifioitu duplikaatiksi.
                    // Ts. jatetaan kierros kesken ja jatketaan seuraavasta julkaisusta
                    //ENDLOOP:;
                   

                }


                reader_SA.Close();
                conn_SA.Close();


                ////////////////////
                // AJASTIN, loppu //
                ////////////////////
                //watch.Stop();
                //var elapsedMs = watch.ElapsedMilliseconds;
                //Console.Write("Koko ajon kesto: " + elapsedMs);


            }

            //Console.Write("Lopussa ollaan");

            //Console.ReadLine();

            Environment.Exit(0);

        }

    }

}