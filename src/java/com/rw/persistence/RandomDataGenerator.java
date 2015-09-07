package com.rw.persistence;

import java.util.HashMap;
import java.util.Random;

public class RandomDataGenerator
{
    /**
     * @param isUS US numbers look different to UK ones
     * @return A phone number
     */
	
    public static String getPhoneNumber(boolean isUS)
    {
        String phoneNumber;
        if (isUS)
        {
            // US
            phoneNumber = "+1 (925) "
                + random.nextInt(9) + random.nextInt(9) + random.nextInt(9) + " - "
                + random.nextInt(9) + random.nextInt(9) + random.nextInt(9) + random.nextInt(9);
        }
        else
        {
            // UK
            phoneNumber = "+44 (0) 1" + random.nextInt(9) + random.nextInt(9) + random.nextInt(9)
                + " " + random.nextInt(9) + random.nextInt(9) + random.nextInt(9) + random.nextInt(9)
                + random.nextInt(9) + random.nextInt(9);
        }
        return phoneNumber;
    }

    public static String getFirstName()
    {
        return FIRSTNAMES[random.nextInt(FIRSTNAMES.length)];
    }

    public static String getSurname()
    {
        return SURNAMES[random.nextInt(SURNAMES.length)];
    }

    public static String getCity()
    {
        return TOWNS[random.nextInt(TOWNS.length)];
    }

    public static String getState()
    {
        return STATES[random.nextInt(STATES.length)];
    }
    
    public static String getZip()
    {
        return ZIP[random.nextInt(ZIP.length)];
    }
    
    public static String getStreetAddress()
    {
        String housenum = (random.nextInt(99) + 1) + " ";
        String road1 = ROADS1[random.nextInt(ROADS1.length)] + " ";
        String road2 = ROADS2[random.nextInt(ROADS2.length)];
        return housenum + road1 + road2;
        
    }
    
    public static String getFullName()
    {
        return getFirstName() + " " + getSurname();
    }

    public static String getAddress()
    {
        String housenum = (random.nextInt(99) + 1) + " ";
        String road1 = ROADS1[random.nextInt(ROADS1.length)];
        String road2 = ROADS2[random.nextInt(ROADS2.length)];
        int townNum = random.nextInt(TOWNS.length);
        String town = TOWNS[townNum];
        return housenum + road1 + " " + road2 + ", " + town;
    }
    
    public static HashMap getRealAddress()
    {
    	HashMap[] addresses = new HashMap[4];
    	
    	addresses[0]=new HashMap();
    	addresses[0].put("streetAddress1", "2040 N Main St");
    	addresses[0].put("cityAddress","Walnut Creek");
    	addresses[0].put("stateAddress","CA");
    	addresses[0].put("postalCodeAddress", "94596");
    	
    	addresses[1]=new HashMap();
    	addresses[1].put("streetAddress1", "1598 Hillgrade Ave");
    	addresses[1].put("cityAddress","Alamo");
    	addresses[1].put("stateAddress","CA");
    	addresses[1].put("postalCodeAddress", "94507");
    	
    	addresses[2]=new HashMap();
    	addresses[2].put("streetAddress1", "863 El Pintado Rd");
    	addresses[2].put("cityAddress","Danville");
    	addresses[2].put("stateAddress","CA");
    	addresses[2].put("postalCodeAddress", "94526");    	
    	
    	addresses[3]=new HashMap();
    	addresses[3].put("streetAddress1", "208 Maui Ct");
    	addresses[3].put("cityAddress","San Ramon");
    	addresses[3].put("stateAddress","CA");
    	addresses[3].put("postalCodeAddress", "94582");
    	
    	return addresses[random.nextInt(4)];

    	
    	
    	
    	
    }

    public static String[] getAddressAndNumber()
    {
        String[] reply = new String[2];

        String housenum = (random.nextInt(99) + 1) + " ";
        String road1 = ROADS1[random.nextInt(ROADS1.length)];
        String road2 = ROADS2[random.nextInt(ROADS2.length)];
        int townNum = random.nextInt(TOWNS.length);
        String town = TOWNS[townNum];

        reply[0] = housenum + road1 + " " + road2 + ", " + town;
        reply[1] = getPhoneNumber(townNum < 5);

        return reply;
    }

    public static float getSalary()
    {
        return Math.round(10 + 90 * random.nextFloat()) * 1000;
    }
    
    public static String getMessage()
    {
        return MESSAGES[random.nextInt(MESSAGES.length)];
    }
 
    public static String getTitle()
    {
        return TITLES[random.nextInt(TITLES.length)];
    }
    
    public static String getTagLine()
    {
        return TAGLINE[random.nextInt(TAGLINE.length)];
    }

    public static String getBusinessType()
    {
        return BusinessType[random.nextInt(BusinessType.length)];
    }
    
    public static String getBusinessName()
    {
        return CompanyNames[random.nextInt(CompanyNames.length)];
    }

    public static String getCompanyURL()
    {
        return CompanyNames[random.nextInt(CompanyNames.length)];
    }

    public static String getPhotoURL()
    {
        return photoURL[random.nextInt(photoURL.length)];
    }

    public static String getlogoURL()
    {
        return logoURL[random.nextInt(logoURL.length)];
    }
 
    public static String getAssoc()
    {
        return AssociationNames[random.nextInt(AssociationNames.length)];
    }
  
    public static final Random random = new Random();

    public static final String[] FIRSTNAMES =
    {
        "Fred", "Jim", "Shiela", "Jack", "Betty", "Jacob", "Martha", "Kelly",
        "Luke", "Matt", "Gemma", "Joe", "Ben", "Jessie", "Leanne", "Becky",
        "William", "Jo","Jill","James","Peter","Mark","Melissa","Andrew","Amanda",
        "Archana","Anshu","Mira","Shalini","Kunchan","Ramin","Shreya","Lucy","Janet",
        "Maggie","Pamela","Yasmin","Cindy"
    };

    public static final String[] SURNAMES =
    {
        "Smith", "MacDonald", "Jones", "Smith", "Cavanaugh", 
        "Sutter", "Davidson", "Schwartz", "Stevenson", "Lee", "Abrams",
        "Danielson", "Walker", "Ryan","Rosenbloom","Chan","Chin","Barnes","Kirk","Davis","Nash",
    };

    public static final String[] ROADS1 =
    {
        "Green", "Red", "Yellow", "Brown", "Blue", "Black", "White", "Magenta"
    };

    public static final String[] ROADS2 =
    {
        "Court", "Drive", "Street", "Avenue", "Crescent", "Road", "Place",
    };

    public static final String[] TOWNS =
    {
        "San Mateo", "San Francisco", "San Diego", "New York", "Atlanta",
        "Sandford", "York", "London", "Coventry", "Exeter", "Knowle", "San Ramon", "New Jersey", "Los Angeles"
    };
    
    public static final String[] STATES =
        {
            "CA"
    	//, "MA", "GA", "PA", "WA",
        //    "NJ", "NY", "AR", "NV", "UT", "TX", "NC", "SC", "OH"
        };
    
    public static final String[] TITLES =
    {
        "President", "Vice President", "Director", "Agent", "Manager", "Architect",
        "Financial Advisor", "Engineer", "Associate Manager", "", "General Manager"
    };
    
    public static final String[] MESSAGES =
    {
            "Hello, How are you", "Its getting hot outside", "Did you goto school today?", "Are you on vacation ?", "All roads lead to Rome",
            "Old habits die hard", "Finished Mapping Kivi Birds", "London bridge falling down", "Twinkle Twinkle little star", 
            "Apple today announced that it has sold over five million of its new iPhone 5", "Download the Apple Store app.",
    };
    
    public static final String[] ZIP =
        {
                "11111", "22222", "33333", "44444", "55555",
                "66666", "77777", "88888", "99999", 
                "12345", "23456","34567"
        };
    
    public static final String[] TAGLINE =
        {
                "Pancake Social", "Investor Briefing", "GOP Fund", "Democratic Convention", "Wine Tasting",
                "Angel Investments", "Birthday Party", "Graduation Party", "User Conference", 
                "Susan Koman 10K", "Soccer Practice","Bike Race"
        };
    public static final String[] BusinessType =
        {
                "Business Type A", "Business Type B", "Business Type C", "Business Type D", "Business Type E",
                "Business Type F", "Business Type G", "Business Type H"
        };
    public static final String[] CompanyNames =
        {
                "Novartis", "BART", "Sleep Train", "EBMUD", "Clorox",
                "Safeway", "Chevron", "Levi Straus",""
        };
  
    public static final String[] AssociationNames =
        {
                "Chanmber of Commerce", "", "Rotary Club", "Investors Association", "Networking Group",
                "Accountants Association", "Teachers Association", "La Vida Group"
        };
  public static final String[] photoURL =
        {
                "http://t0.gstatic.com/images?q=tbn:ANd9GcQw0n59Hl2a5pXDTjoHhgJzC-htH6pTZ54w3Um9ejRGyaRkMQchvA", 
                "http://t1.gstatic.com/images?q=tbn:ANd9GcRW_4iEAA96_DDNwVU81pizJa6YzW6fpOIZf1RMVVRbm5DVQsdG", 
                "http://t2.gstatic.com/images?q=tbn:ANd9GcT1Y_c2yzTmZqY1mF_GBhtIxoJ1_nKzSJaU4d-naQTFiUmouWZz", 
                "http://t1.gstatic.com/images?q=tbn:ANd9GcTxsO9or9Bhb5CT2MsvL4IYy_81zHV-PbGyZsDMxEuK9TDe6Z-hiQ", 
                "http://t0.gstatic.com/images?q=tbn:ANd9GcQWBNUJ8Ug4ZlVI0-Pkg5wGTy1JlWj1iT4bH7TwxjV2C9YS8zHs",
                "http://t2.gstatic.com/images?q=tbn:ANd9GcQbasLUpjUtia3hsnzY3BsxEr-krhKPR2mcXrFal6Bgv11ZxlulMA", 
                "http://t3.gstatic.com/images?q=tbn:ANd9GcT0KNXuPkT3km7WEqC63nMu0xn5luwlhsis-hlubJ-riu4NWSKghQ", 
                "http://t3.gstatic.com/images?q=tbn:ANd9GcTHKGK8jOCf4fQTrRFqqv98qTqIHYxuhawJTRyQx61oUq4N9mtAUA"
        };
  public static final String[] logoURL =
      {
	  	"http://t2.gstatic.com/images?q=tbn:ANd9GcQjf6qeSi293DNsoL-soZ-fZtbRSdfoUEX3Dsi-_0lBlrn2zLVv",
	  	"http://t0.gstatic.com/images?q=tbn:ANd9GcQlwKHDuVZeTgKDItsqb3lyNAMk_xIn3EeQmAAElP-50MsAfehEzg",
	  	"http://t2.gstatic.com/images?q=tbn:ANd9GcRslCwYWiRU4YUnD2vpYcbTb98W7ACqSJorStUpS6TD2ZnGFoP5",
	  	"http://t3.gstatic.com/images?q=tbn:ANd9GcQ8D4s3W4x9oZFh-KBZa8hdy-GhiQTpRMJmKfeBUXwqGSBHpzTv",
	  	"http://t0.gstatic.com/images?q=tbn:ANd9GcRkaB0BJlhnpzzzwEcaWzZyL9iAZowPky9FGFWkYKWCYCivha8R",
	  	"http://t1.gstatic.com/images?q=tbn:ANd9GcS0-acfkDNF8XRIdT0oyBbXGz-Qq1h47NWOmrm3se1XygSVnM6svw",
	  	"http://t1.gstatic.com/images?q=tbn:ANd9GcQT8Vr8VmOCpszxj9CTZ2Cyj3CmCL6KQBXJghppF_e3APN0W7nZ_g"
      };
}

