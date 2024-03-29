package com.dreams.video.upload;

import com.dreams.video.entity.Video;
import com.dreams.video.form.FileForm;
import com.dreams.video.form.VideoForm;
import org.springframework.stereotype.Controller;
import org.springframework.util.StringUtils;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.ModelAttribute;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.multipart.MultipartFile;

import java.io.File;
import java.io.IOException;


@Controller
@RequestMapping("/video")
public class UploadVideo {

    @CrossOrigin(origins = "*", maxAge = 3600)
    @RequestMapping(value = "/uploadfile")
    @ResponseBody
    public String uploadFile(@ModelAttribute FileForm videoForm){
        String uploadpath = "C:\\projects\\upload\\"+videoForm.getFile().getOriginalFilename();
        MultipartFile file = videoForm.getFile();
        try {
//            InputStream inputStream = videoForm.getFile().getInputStream();
//            FileUtils.copyToFile(inputStream,new File(uploadpath));

            file.transferTo(new File(uploadpath));
        } catch (IOException e) {
            e.printStackTrace();
        }
        String fileName = videoForm.getFile().getName();
        return fileName;
    }


    public String uploadVideoForm(VideoForm form){
        Video video = new Video();
        if(!StringUtils.isEmpty(form.getTitle())){
            video.setTitle(form.getTitle());
        }



        return "成功";
    }
}
