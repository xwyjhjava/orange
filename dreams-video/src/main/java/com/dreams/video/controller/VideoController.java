package com.dreams.video.controller;

import java.util.ArrayList;
import java.util.List;

public class VideoController implements IVideoController {

    private String VideoName;

    int videoURL = 0;



    @Override
    public void getVideo() {


        System.out.println();

        System.out.println("true = " + true);
        System.out.println("VideoController.getVideo");


        List<Integer> list = new ArrayList<>();
        list.add(1);
        list.add(2);
        list.add(3);


    }

    @Override
    public void getName() {

    }

    @Override
    public String toString() {
        return super.toString();
    }
}
